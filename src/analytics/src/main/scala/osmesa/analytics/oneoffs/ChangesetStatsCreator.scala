package osmesa.analytics.oneoffs

import java.net.URI
import java.sql._

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import osmesa.analytics.Analytics
import osmesa.analytics.stats._
import osmesa.analytics.stats.functions._
import vectorpipe.{internal => ProcessOSM}
import vectorpipe.functions._
import vectorpipe.functions.osm._
import vectorpipe.util.{DBUtils, Geocode}

object ChangesetStatsCreator
    extends CommandApp(
      name = "changeset-stats",
      header = "Changeset statistics",
      main = {
        val historyOpt =
          Opts.option[String]("history", help = "Location of the History ORC file to process.")

        val changesetsOpt =
          Opts
            .option[String]("changesets", help = "Location of the Changesets ORC file to process.")

      val changesetBaseOpt =
        Opts
          .option[URI](
          "changeset-stream",
          short = "c",
          metavar = "uri",
          help = "HTTP Location of replication changesets"
        )
        .validate("Changeset source must have trailing '/'") { _.getPath.endsWith("/") }

        val databaseUrlOpt =
          Opts
            .option[URI](
              "database-url",
              short = "d",
              metavar = "database URL",
              help = "Database URL (default: DATABASE_URL environment variable)"
            )
            .orElse(Opts.env[URI]("DATABASE_URL", help = "The URL of the database"))

        (historyOpt, changesetsOpt, changesetBaseOpt, databaseUrlOpt).mapN {
          (historySource, changesetSource, changesetBaseURI, databaseUrl) =>
            implicit val spark: SparkSession = Analytics.sparkSession("ChangesetStats")
            import spark.implicits._

            val logger = org.apache.log4j.Logger.getLogger(getClass())

            val history = spark.read.orc(historySource)

            val augdiffEndSequence = {
              val t = history.select(max('timestamp)).first.getAs[java.sql.Timestamp](0).toInstant
              ((t.getEpochSecond - 1347432900) / 60).toInt
            }

            val nodes = ProcessOSM.preprocessNodes(history)
            val ways = ProcessOSM.preprocessWays(history)

            val pointGeoms = Geocode(
              ProcessOSM
                .constructPointGeometries(
                  // pre-filter to tagged nodes
                  nodes.where(isTagged('tags))
                )
                .withColumn("minorVersion", lit(0)))

            val wayGeoms = Geocode(
              ProcessOSM
                .reconstructWayGeometries(
                  // pre-filter to tagged ways
                  ways.where(isTagged('tags)),
                  // let reconstructWayGeometries do its thing; nodes are cheap
                  nodes
                )
                .drop('geometryChanged))

            val augmentedWays = wayGeoms.withPrevGeom.withDelta

            val wayChangesetStats = augmentedWays
              .select(
                'changeset,
                'countries,
                DefaultMeasurements,
                DefaultCounts
              )
              .groupBy('changeset)
              .agg(
                sum_measurements(collect_list('measurements)) as 'measurements,
                sum_counts(collect_list('counts)) as 'counts,
                count_values(flatten(collect_list('countries))) as 'countries
              )

            val pointChangesetStats = pointGeoms
              .select(
                'changeset,
                'countries,
                pointCounts
              )
              .groupBy('changeset)
              .agg(
                sum_counts(collect_list('counts)) as 'counts,
                count_values(flatten(collect_list('countries))) as 'countries
              )

            val changesetStats = wayChangesetStats
              .join(pointChangesetStats, Seq("changeset"), "full_outer")
              .withColumn("mergedCounts",
                          merge_counts(wayChangesetStats("counts"), pointChangesetStats("counts")))
              .select(
                'changeset,
                'measurements,
                'mergedCounts as 'counts,
                sum_count_values('mergedCounts) as 'totalEdits,
                merge_counts(wayChangesetStats("countries"), pointChangesetStats("countries")) as 'countries
              )

            val changesets = spark.read.orc(changesetSource)
            val changesetsEndSequence = {
              val t = changesets.select(max(coalesce('createdAt, 'closedAt))).first.getAs[java.sql.Timestamp](0)
              ChangesetORCUpdaterUtils.findSequenceFor(t.toInstant, changesetBaseURI).toInt
            }

            val changesetMetadata = changesets
              .groupBy('id,
                       'tags.getItem("created_by") as 'editor,
                       'uid,
                       'user,
                       'createdAt,
                       'tags.getItem("comment") as 'comment,
                       'tags.getItem("hashtags") as 'hashtags)
              .agg(first('closedAt, ignoreNulls = true) as 'closedAt)
              .select(
                'id,
                'editor,
                'uid,
                'user,
                'createdAt,
                'closedAt,
                merge_sets(hashtags('comment), hashtags('hashtags)) as 'hashtags
              )

            changesetStats.foreachPartition(rows => {
              val writer = new ChangesetStatsForeachWriter(databaseUrl)

              if (writer.open(TaskContext.getPartitionId(), 0)) {
                try {
                  rows.foreach(writer.process)

                  writer.close(null)
                } catch {
                  case e: Throwable => writer.close(e)
                }
              }
            })

            changesetMetadata
              .orderBy('hashtags)
              .foreachPartition(rows => {
                val writer = new ChangesetMetadataForeachWriter(databaseUrl)

                if (writer.open(TaskContext.getPartitionId(), 0)) {
                  try {
                    rows.foreach(writer.process)

                    writer.close(null)
                  } catch {
                    case e: Throwable => writer.close(e)
                  }
                }
              })

            // Distributing these writes to the executors to avoid no suitable driver errors on master node
            logger.warn(s"Writing AugmentedDiffStream sequence number as $augdiffEndSequence to $databaseUrl")
            spark.sparkContext.parallelize(Seq(databaseUrl)).foreach { uri =>
              ChangesetORCUpdaterUtils.saveLocations("AugmentedDiffStream", augdiffEndSequence, uri)
            }
            logger.warn(s"Writing ChangesetStream sequence number as $changesetsEndSequence to $databaseUrl")
            spark.sparkContext.parallelize(Seq(databaseUrl)).foreach { uri =>
              ChangesetORCUpdaterUtils.saveLocations("ChangesetStream", changesetsEndSequence, uri)
            }

            spark.stop()
        }
      }
    )
