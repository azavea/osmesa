package osmesa.apps.batch

import java.net.URI

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import osmesa.analytics.Analytics
import osmesa.analytics.stats._
import osmesa.analytics.stats.functions._
import osmesa.apps.DbUtils
import vectorpipe.functions.{flatten => _, _}
import vectorpipe.functions.osm._
import vectorpipe.sources.{AugmentedDiffSource, ChangesetSource}
import vectorpipe.util.Geocode
import vectorpipe.{internal => ProcessOSM}

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

        val statsCheckpointOpt =
          Opts
            .option[URI](
              "stats-checkpoint",
              metavar = "ORC URI",
              help = "Location to save ORC file of stats derived from history file"
            )
            .orNone

        val useCheckpointOpt =
          Opts
            .flag(
              "use-checkpoint",
              help = "Use saved stats checkpoint; don't process history"
            )
            .orFalse

        val maxConnectionsOpt =
          Opts
            .option[Int](
              "max-connections",
              metavar = "n connections",
              help = "Maximum number of PostgreSQL connections to use"
            )
            .withDefault(64)

        val databaseUrlOpt =
          Opts
            .option[URI](
              "database-url",
              short = "d",
              metavar = "database URL",
              help = "Database URL (default: DATABASE_URL environment variable)"
            )
            .orElse(Opts.env[URI]("DATABASE_URL", help = "The URL of the database"))

        (historyOpt, changesetsOpt, changesetBaseOpt, databaseUrlOpt, statsCheckpointOpt, useCheckpointOpt, maxConnectionsOpt).mapN {
          (historySource, changesetSource, changesetBaseURI, databaseUrl, statsCheckpoint, useCheckpoint, maxConnections) =>
            implicit val spark: SparkSession = Analytics.sparkSession("ChangesetStats")
            import spark.implicits._

            val logger = org.apache.log4j.Logger.getLogger(getClass())

            val history = spark.read.orc(historySource)

            val augdiffEndSequence = {
              val t = history.select(max('timestamp)).first.getAs[java.sql.Timestamp](0)
              AugmentedDiffSource.timestampToSequence(t)
            }

            val changesetStats =
              if (!useCheckpoint) {
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

                val augmentedWays = wayGeoms.withPrevGeom.withLinearDelta.withAreaDelta

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
                  DefaultCounts
                )
                .groupBy('changeset)
                .agg(
                  sum_counts(collect_list('counts)) as 'counts,
                  count_values(flatten(collect_list('countries))) as 'countries
                )

                val stats = wayChangesetStats
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
                .persist(StorageLevel.DISK_ONLY)

                if (statsCheckpoint isDefined) {
                  stats
                    .write
                    .option("orc.compress", "snappy")
                    .mode(SaveMode.Overwrite)
                    .orc(statsCheckpoint.get.toString)
                }

                stats
              } else {
                if (statsCheckpoint isDefined) {
                  spark
                    .read
                    .orc(statsCheckpoint.get.toString)
                } else {
                  throw new IllegalArgumentException("No stats checkpoint was defined")
                }
              }

            val changesets = spark.read.orc(changesetSource)
            val changesetsEndSequence = {
              val t = changesets.select(max(coalesce('createdAt, 'closedAt))).first.getAs[java.sql.Timestamp](0)
              ChangesetSource.findSequenceFor(t.toInstant, changesetBaseURI).toInt
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

            changesetStats.repartition(maxConnections).foreachPartition(rows => {
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
              .repartition(maxConnections)
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
            logger.warn(s"Writing augmented diff sequence number as $augdiffEndSequence to $databaseUrl")
            spark.sparkContext.parallelize(Seq(databaseUrl)).foreach { uri =>
              DbUtils.saveLocations("ChangesetStatsUpdater", augdiffEndSequence, uri)
            }
            logger.warn(s"Writing changeset stream sequence number as $changesetsEndSequence to $databaseUrl")
            spark.sparkContext.parallelize(Seq(databaseUrl)).foreach { uri =>
              DbUtils.saveLocations("ChangesetMetadataUpdater", changesetsEndSequence, uri)
            }

            spark.stop()
        }
      }
    )
