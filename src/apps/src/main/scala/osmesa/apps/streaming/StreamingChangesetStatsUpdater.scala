package osmesa.apps.streaming

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import geotrellis.vector.{Feature, Geometry}
import _root_.io.circe._
import _root_.io.circe.syntax._
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import osmesa.analytics.Analytics
import osmesa.analytics.stats._
import osmesa.analytics.stats.functions._
import vectorpipe.functions.{flatten => _, _}
import vectorpipe.functions.osm._
import vectorpipe.model.{AugmentedDiff, ElementWithSequence}
import vectorpipe.sources.{AugmentedDiffSource, AugmentedDiffSourceErrorHandler, Source}
import vectorpipe.util.{DBUtils, Geocode, RobustFeature}

/*
 * Usage example:
 *
 * sbt "project apps" assembly
 *
 * spark-submit \
 *   --class osmesa.apps.streaming.StreamingChangesetStatsUpdater \
 *   ingest/target/scala-2.11/osmesa-apps.jar \
 *   --augmented-diff-source s3://somewhere/diffs/ \
 *   --database-url $DATABASE_URL
 */
object StreamingChangesetStatsUpdater
    extends CommandApp(
      name = "osmesa-augmented-diff-stream-processor",
      header = "Update statistics from streaming augmented diffs",
      main = {
        type AugmentedDiffFeature = Feature[Geometry, ElementWithSequence]

        val augmentedDiffSourceOpt =
          Opts
            .option[URI](
              "augmented-diff-source",
              short = "a",
              metavar = "uri",
              help = "Location of augmented diffs to process"
            )

        val databaseUriOpt =
          Opts
            .option[URI](
              "database-url",
              short = "d",
              metavar = "database URL",
              help = "Database URL (default: $DATABASE_URL environment variable)"
            )
            .orElse(Opts.env[URI]("DATABASE_URL", help = "The URL of the database"))

        val startSequenceOpt =
          Opts
            .option[Int](
              "start-sequence",
              short = "s",
              metavar = "sequence",
              help = "Starting sequence. If absent, the current (remote) sequence will be used."
            )
            .orNone

        val endSequenceOpt =
          Opts
            .option[Int]("end-sequence",
                         short = "e",
                         metavar = "sequence",
                         help = "Ending sequence. If absent, this will be an infinite stream.")
            .orNone

        val batchSizeOpt = Opts
          .option[Int]("batch-size",
                       short = "b",
                       metavar = "batch size",
                       help = "Change batch size.")
          .orNone

        (augmentedDiffSourceOpt, startSequenceOpt, endSequenceOpt, databaseUriOpt, batchSizeOpt)
          .mapN {
            (augmentedDiffSource, startSequence, endSequence, databaseUri, batchSize) =>
              implicit val ss: SparkSession =
                Analytics.sparkSession("StreamingChangesetStatsUpdater")

              import ss.implicits._

              val options = Map(
                Source.BaseURI -> augmentedDiffSource.toString,
                Source.DatabaseURI -> databaseUri.toString,
                Source.ProcessName -> "ChangesetStatsUpdater",
                Source.ErrorHandler -> "osmesa.apps.streaming.LogErringFeatures"
              ) ++
                startSequence
                  .map(s => Map(Source.StartSequence -> s.toString))
                  .getOrElse(Map.empty) ++
                endSequence
                  .map(s => Map(Source.EndSequence -> s.toString))
                  .getOrElse(Map.empty) ++
                batchSize
                  .map(x => Map(Source.BatchSize -> x.toString))
                  .getOrElse(Map.empty)

              val geoms = ss.readStream.format(Source.AugmentedDiffs).options(options).load

              // aggregations are triggered when an event with a later timestamp ("event time") is received
              // in practice, this means that aggregation doesn't occur until the *next* sequence is received

              val query = Geocode(geoms.where(isTagged('tags)))
                .withColumn("timestamp", AugmentedDiffSource.sequenceToTimestamp('sequence))
                // if sequences are received sequentially (and atomically), 0 seconds should suffice; anything received with an
                // earlier timestamp after that point will be dropped
                .withWatermark("timestamp", "0 seconds")
                .withLinearDelta
                .withAreaDelta
                .select(
                  'timestamp,
                  'sequence,
                  'changeset,
                  'uid,
                  'user,
                  'countries,
                  DefaultMeasurements,
                  DefaultCounts
                )
                .groupBy('timestamp, 'sequence, 'changeset, 'uid, 'user)
                .agg(
                  sum_measurements(collect_list('measurements)) as 'measurements,
                  sum_counts(collect_list('counts)) as 'counts,
                  count_values(flatten(collect_list('countries))) as 'countries
                )
                .withColumn("totalEdits", sum_count_values('counts))
                .writeStream
                .queryName("aggregate statistics by sequence")
                .foreach(new ChangesetStatsForeachWriter(databaseUri, shouldUpdateUsernames = true))
                .start

              query.awaitTermination()

              ss.stop()
          }
      }
    )

class LogErringFeatures extends AugmentedDiffSourceErrorHandler with Logging {

  val updateErrorLog: String =
    """
      |WITH data AS (
      |  SELECT
      |    ? AS id,
      |    ? as type,
      |    ? as sequence,
      |    ?::jsonb as tags,
      |    ? as nds,
      |    ? as changeset,
      |    ? as uid,
      |    ? as "user",
      |    ?::timestamp with time zone as updated,
      |    ? as visible,
      |    ? as version
      |)
      |INSERT INTO errors AS c (
      |  id,
      |  type,
      |  sequence,
      |  tags,
      |  nds,
      |  changeset,
      |  uid,
      |  "user",
      |  updated,
      |  visible,
      |  version
      |) SELECT * FROM data
      |ON CONFLICT DO NOTHING
    """.stripMargin

  var databaseUri: URI = null

  override def setOptions(options: Map[String, String]): Unit = {
    databaseUri = options.get(Source.DatabaseURI).map(new URI(_)).getOrElse(null)
  }

  override def handle(sequence: Int, feature: RobustFeature[Geometry, ElementWithSequence]): Unit = {
    logError(s"Found erring feature in sequence ${sequence}: ${feature}")
    if (databaseUri != null) {
      val connection = DBUtils.getJdbcConnection(databaseUri)
      val statement = connection.prepareStatement(updateErrorLog)
      val adiff = AugmentedDiff(sequence, None, feature.toFeature)

      statement.setLong(1, adiff.id)
      statement.setShort(2, adiff.`type`)
      statement.setInt(3, adiff.sequence)
      statement.setString(4, adiff.tags.asJson.noSpaces)

      val nodes = connection.createArrayOf("bigint", adiff.nds.map(_.underlying).toArray)

      statement.setArray(5, nodes)
      statement.setLong(6, adiff.changeset)
      statement.setLong(7, adiff.uid)
      statement.setString(8, adiff.user)
      statement.setTimestamp(9, adiff.updated)
      statement.setBoolean(10, adiff.visible)
      statement.setInt(11, adiff.version)
      statement.addBatch()

      if (statement.executeBatch().exists(_ == 0))
        logError(s"Failed to add feature to error table!!")
    }
  }
}
