package osmesa.analytics.oneoffs

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql._
import osmesa.analytics.Analytics
import vectorpipe.functions._
import vectorpipe.functions.osm._
import vectorpipe.sources.Source
import vectorpipe.util.DBUtils
import osmesa.analytics.stats.ChangesetMetadataForeachWriter

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.StreamingChangesetMetadataUpdater \
 *   ingest/target/scala-2.11/osmesa-analytics.jar \
 *   --database-url $DATABASE_URL
 */
object StreamingChangesetMetadataUpdater
    extends CommandApp(
      name = "osmesa-augmented-diff-stream-processor",
      header = "Update statistics from streaming augmented diffs",
      main = {
        val changesetSourceOpt =
          Opts
            .option[URI]("changeset-source",
                         short = "c",
                         metavar = "uri",
                         help = "Location of changesets to process")
            .withDefault(new URI("https://planet.osm.org/replication/changesets/"))

        val databaseUrlOpt =
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
            .option[Int](
              "end-sequence",
              short = "e",
              metavar = "sequence",
              help = "Ending sequence. If absent, this will be an infinite stream."
            )
            .orNone

        val batchSizeOpt = Opts
          .option[Int]("batch-size",
                       short = "b",
                       metavar = "batch size",
                       help = "Change batch size.")
          .orNone

        (changesetSourceOpt, databaseUrlOpt, startSequenceOpt, endSequenceOpt, batchSizeOpt).mapN {
          (changesetSource, databaseUrl, startSequence, endSequence, batchSize) =>
            implicit val ss: SparkSession =
              Analytics.sparkSession("StreamingChangesetMetadataUpdater")

            import ss.implicits._

            val options = Map(
              Source.BaseURI -> changesetSource.toString,
              Source.DatabaseURI -> databaseUrl.toString,
              Source.ProcessName -> "ChangesetMetadataUpdater"
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

            val changesets =
              ss.readStream
                .format(Source.Changesets)
                .options(options)
                .load

            val changesetProcessor = changesets
              .select(
                'id,
                'createdAt,
                'closedAt,
                'user,
                'uid,
                'tags.getField("created_by") as 'editor,
                merge_sets(hashtags('tags.getField("comment")),
                           hashtags('tags.getField("hashtags"))) as 'hashtags
              )
              .writeStream
              .queryName("update changeset metadata")
              .foreach(new ChangesetMetadataForeachWriter(databaseUrl,
                                                          shouldUpdateUsernames = true))
              .start

            changesetProcessor.awaitTermination()

            ss.stop()
        }
      }
    )
