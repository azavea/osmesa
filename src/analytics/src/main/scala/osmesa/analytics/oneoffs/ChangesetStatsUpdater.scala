package osmesa.analytics.oneoffs

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.TaskContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import osmesa.analytics.Analytics
import osmesa.analytics.stats._
import osmesa.analytics.stats.functions._
import vectorpipe.{internal => ProcessOSM}
import vectorpipe.model.ElementWithSequence
import vectorpipe.sources.Source
import vectorpipe.util.Geocode

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.ChangesetStatsUpdater \
 *   ingest/target/scala-2.11/osmesa-analytics.jar \
 *   --augmented-diff-source s3://somewhere/diffs/ \
 *   --database-url $DATABASE_URL
 */
object ChangesetStatsUpdater
    extends CommandApp(
      name = "osmesa-changeset-stats-updater",
      header = "Update statistics from augmented diffs",
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
            .option[Int]("end-sequence",
                         short = "e",
                         metavar = "sequence",
                         help = "Ending sequence. If absent, this will be an infinite stream.")
            .orNone

        val partitionCountOpt = Opts
          .option[Int]("partition-count",
                       short = "p",
                       metavar = "partition count",
                       help = "Change partition count.")
          .orNone

        (augmentedDiffSourceOpt,
         startSequenceOpt,
         endSequenceOpt,
         databaseUrlOpt,
         partitionCountOpt).mapN {
          (augmentedDiffSource, startSequence, endSequence, databaseUrl, partitionCount) =>
            implicit val ss: SparkSession = Analytics.sparkSession("ChangesetStatsUpdater")

            import ss.implicits._

            val options = Map(
              Source.BaseURI -> augmentedDiffSource.toString
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

            val geoms = ss.read.format(Source.AugmentedDiffs).options(options).load

            Geocode(geoms.where(isTagged('tags)))
              .withDelta
              .select(
                'sequence,
                'changeset,
                'uid,
                'user,
                'countries,
                DefaultMeasurements,
                DefaultCounts
              )
              .groupBy('sequence, 'changeset, 'uid, 'user)
              .agg(
                sum_measurements(collect_list('measurements)) as 'measurements,
                sum_counts(collect_list('counts)) as 'counts,
                count_values(flatten(collect_list('countries))) as 'countries
              )
              .withColumn("totalEdits", sum_count_values('counts))
              .foreachPartition(rows => {
                val writer =
                  new ChangesetStatsForeachWriter(databaseUrl, shouldUpdateUsernames = true)

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
