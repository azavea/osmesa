package osmesa.analytics.oneoffs

import java.io._
import java.net.URI

import cats.implicits._
import com.monovore.decline._
import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.sql._
import osmesa.analytics.{Analytics, Footprints}
import osmesa.common.ProcessOSM
import osmesa.common.model._

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.UserFootprintUpdater \
 *   ingest/target/scala-2.11/osmesa-analytics.jar
 */
object UserFootprintUpdater
    extends CommandApp(
      name = "osmesa-user-footprint-updater",
      header = "Consume minutely diffs + changesets and update user footprint MVTs",
      main = {
        type AugmentedDiffFeature = Feature[Geometry, ElementWithSequence]
        val rootURI = new File("").toURI

        val changeSourceOpt = Opts
          .option[URI]("change-source",
                       short = "d",
                       metavar = "uri",
                       help = "Location of minutely diffs to process")
          .withDefault(new URI("https://planet.osm.org/replication/minute/"))
        val changesStartSequenceOpt = Opts
          .option[Int](
            "changes-start-sequence",
            short = "s",
            metavar = "sequence",
            help =
              "Minutely diff starting sequence. If absent, the current (remote) sequence will be used.")
          .orNone
        val changesEndSequenceOpt = Opts
          .option[Int](
            "changes-end-sequence",
            short = "e",
            metavar = "sequence",
            help = "Minutely diff ending sequence. If absent, this will be an infinite stream.")
          .orNone
        val changesBatchSizeOpt = Opts
          .option[Int]("changes-batch-size",
                       short = "b",
                       metavar = "batch size",
                       help = "Change batch size.")
          .orNone
        val tileSourceOpt = Opts
          .option[URI](
            "tile-source",
            short = "t",
            metavar = "uri",
            help = "URI prefix for vector tiles to update"
          )
          .withDefault(rootURI)
        val concurrentUploadsOpt = Opts
          .option[Int]("concurrent-uploads",
                       short = "c",
                       metavar = "concurrent uploads",
                       help = "Set the number of concurrent uploads.")
          .orNone
        val databaseUriOpt =
          Opts.option[URI](
            "database-uri",
            short = "d",
            metavar = "database URL",
            help = "Database URL (default: $DATABASE_URL environment variable)"
          )
        val databaseUriEnv =
          Opts.env[URI]("DATABASE_URL", help = "The URL of the database")

        (changeSourceOpt,
         changesStartSequenceOpt,
         changesEndSequenceOpt,
         changesBatchSizeOpt,
         tileSourceOpt,
         concurrentUploadsOpt,
         databaseUriOpt orElse databaseUriEnv).mapN {
          (changeSource,
           changesStartSequence,
           changesEndSequence,
           changesBatchSize,
           tileSource,
           _concurrentUploads,
           databaseUri) =>
            val spark: SparkSession = Analytics.sparkSession("UserFootprintUpdater")
            import spark.implicits._
            implicit val concurrentUploads: Option[Int] = _concurrentUploads

            val changeOptions = Map("base_uri" -> changeSource.toString,
                                    "db_uri" -> databaseUri.toString,
                                    "proc_name" -> "UserFootprintUpdater") ++
              changesStartSequence
                .map(s => Map("start_sequence" -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              changesEndSequence
                .map(s => Map("end_sequence" -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              changesBatchSize
                .map(s => Map("batch_size" -> s.toString))
                .getOrElse(Map.empty[String, String])

            val changes = spark.readStream
              .format("changes")
              .options(changeOptions)
              .load

            val changedNodes = changes
              .where('_type === ProcessOSM.NodeType and 'lat.isNotNull and 'lon.isNotNull)
              .select('sequence, 'user, 'lat, 'lon)

            val tiledNodes =
              Footprints.updateFootprints(tileSource,
                                          changedNodes
                                            .withColumnRenamed("user", "key"))

            val query = tiledNodes.writeStream
              .queryName("tiled user footprints")
              .format("console")
              .start

            query.awaitTermination()

            spark.stop()
        }
      }
    )
