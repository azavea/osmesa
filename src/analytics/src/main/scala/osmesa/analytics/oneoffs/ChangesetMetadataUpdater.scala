package osmesa.analytics.oneoffs

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.TaskContext
import org.apache.spark.sql._
import osmesa.analytics.Analytics
import osmesa.analytics.stats.ChangesetMetadataForeachWriter
import vectorpipe.functions._
import vectorpipe.functions.osm._
import vectorpipe.sources.Source

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.ChangesetMetadataUpdater \
 *   ingest/target/scala-2.11/osmesa-analytics.jar \
 *   --database-url $DATABASE_URL
 */
object ChangesetMetadataUpdater
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

        val partitionCountOpt = Opts
          .option[Int]("partition-count",
                       short = "p",
                       metavar = "partition count",
                       help = "Change partition count.")
          .orNone

        (changesetSourceOpt, databaseUrlOpt, startSequenceOpt, endSequenceOpt, partitionCountOpt)
          .mapN {
            (changesetSource, databaseUrl, startSequence, endSequence, partitionCount) =>
              implicit val ss: SparkSession = Analytics.sparkSession("ChangesetMetadataUpdater")

              import ss.implicits._

              val options = Map(
                Source.BaseURI -> changesetSource.toString,
                Source.ProcessName -> "ChangesetStream"
              ) ++
                startSequence
                  .map(s => Map(Source.StartSequence -> s.toString))
                  .getOrElse(Map.empty) ++
                endSequence
                  .map(s => Map(Source.EndSequence -> s.toString))
                  .getOrElse(Map.empty) ++
                partitionCount
                  .map(x => Map(Source.PartitionCount -> x.toString))
                  .getOrElse(Map.empty)

              val changesets =
                ss.read
                  .format(Source.Changesets)
                  .options(options)
                  .load

              changesets
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
                .foreachPartition(rows => {
                  val writer =
                    new ChangesetMetadataForeachWriter(databaseUrl, shouldUpdateUsernames = true)

                  if (writer.open(TaskContext.getPartitionId(), 0)) {
                    try {
                      rows.foreach(writer.process)

                      writer.close(null)
                    } catch {
                      case e: Throwable => writer.close(e)
                    }
                  }
                })

              ss.stop()
          }
      }
    )
