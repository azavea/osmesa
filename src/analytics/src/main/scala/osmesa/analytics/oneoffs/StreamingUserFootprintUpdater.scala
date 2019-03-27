package osmesa.analytics.oneoffs

import java.io._
import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql._
import osmesa.analytics.{Analytics, Footprints}
import osmesa.common.sources.Source

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.StreamingUserFootprintUpdater \
 *   ingest/target/scala-2.11/osmesa-analytics.jar
 */
object StreamingUserFootprintUpdater
    extends CommandApp(
      name = "osmesa-user-footprint-updater",
      header = "Consume minutely diffs to update user footprint MVTs",
      main = {
        val changeSourceOpt = Opts
          .option[URI]("change-source",
                       short = "d",
                       metavar = "uri",
                       help = "Location of minutely diffs to process")
          .withDefault(new URI("https://planet.osm.org/replication/minute/"))

        val startSequenceOpt = Opts
          .option[Int](
            "start-sequence",
            short = "s",
            metavar = "sequence",
            help =
              "Minutely diff starting sequence. If absent, the current (remote) sequence will be used.")
          .orNone

        val batchSizeOpt = Opts
          .option[Int]("batch-size",
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
          .withDefault(new File("").toURI)

        val concurrentUploadsOpt = Opts
          .option[Int]("concurrent-uploads",
                       short = "c",
                       metavar = "concurrent uploads",
                       help = "Set the number of concurrent uploads.")
          .orNone

        val databaseUrlOpt =
          Opts
            .option[URI](
              "database-url",
              short = "d",
              metavar = "database URL",
              help = "Database URL (default: DATABASE_URL environment variable)"
            )
            .orNone

        val databaseUrlEnv =
          Opts.env[URI]("DATABASE_URL", help = "The URL of the database").orNone

        (changeSourceOpt,
         startSequenceOpt,
         batchSizeOpt,
         tileSourceOpt,
         concurrentUploadsOpt,
         databaseUrlOpt orElse databaseUrlEnv).mapN {
          (changeSource, startSequence, batchSize, tileSource, _concurrentUploads, databaseUrl) =>
            val AppName = "UserFootprintUpdater"

            val spark: SparkSession = Analytics.sparkSession(AppName)
            import spark.implicits._
            implicit val concurrentUploads: Option[Int] = _concurrentUploads

            val changeOptions = Map(Source.BaseURI -> changeSource.toString,
                                    Source.ProcessName -> AppName) ++
              databaseUrl
                .map(x => Map(Source.DatabaseURI -> x.toString))
                .getOrElse(Map.empty[String, String]) ++
              startSequence
                .map(x => Map(Source.StartSequence -> x.toString))
                .getOrElse(Map.empty[String, String]) ++
              batchSize
                .map(x => Map(Source.BatchSize -> x.toString))
                .getOrElse(Map.empty[String, String])

            val changes = spark.readStream
              .format(Source.Changes)
              .options(changeOptions)
              .load

            val changedNodes = changes
              .where('type === "node" and 'lat.isNotNull and 'lon.isNotNull)
              .select('sequence, 'uid as 'key, 'lat, 'lon)

            val tiledNodes =
              Footprints.updateFootprints(changedNodes, tileSource)

            val query = tiledNodes.writeStream
              .queryName("tiled user footprints")
              .format("console")
              .start

            query.awaitTermination()

            spark.stop()
        }
      }
    )
