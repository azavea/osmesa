package osmesa.analytics.oneoffs

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import osmesa.analytics.Analytics
import osmesa.analytics.stats._
import osmesa.analytics.stats.functions._
import osmesa.common.ProcessOSM
import osmesa.common.functions.osm.isTagged
import osmesa.common.model.ElementWithSequence
import osmesa.common.sources.Source

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.StreamingChangesetStatsUpdater \
 *   ingest/target/scala-2.11/osmesa-analytics.jar \
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
                Source.ProcessName -> "ChangesetStatsUpdater"
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

              val query = ProcessOSM
                .geocode(geoms.where(isTagged('tags)))
                .withColumn("timestamp", to_timestamp('sequence * 60 + 1347432900))
                // if sequences are received sequentially (and atomically), 0 seconds should suffice; anything received with an
                // earlier timestamp after that point will be dropped
                .withWatermark("timestamp", "0 seconds")
                .withDelta
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
