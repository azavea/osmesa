package osmesa.analytics.oneoffs

import java.io._
import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.locationtech.geomesa.spark.jts._
import osmesa.analytics.{Analytics, EditHistogram}
import osmesa.common.sources.Source

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.StreamingEditHistogramTileUpdater \
 *   ingest/target/scala-2.11/osmesa-analytics.jar
 */
object StreamingEditHistogramTileUpdater
    extends CommandApp(
      name = "osmesa-edit-histogram-tile-updater",
      header = "Consume minutely diffs to update edit histogram MVTs",
      main = {
        val changeSourceOpt = Opts
          .option[URI]("source",
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
            help = "URI prefix of MVTs to update"
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
            .orElse(Opts.env[URI]("DATABASE_URL", help = "The URL of the database"))
            .orNone

        val baseZoomOpt = Opts
          .option[Int]("base-zoom",
                       short = "z",
                       metavar = "Base zoom",
                       help = "Most detailed zoom level")
          .orNone

        (changeSourceOpt,
         startSequenceOpt,
         batchSizeOpt,
         tileSourceOpt,
         concurrentUploadsOpt,
         databaseUrlOpt,
         baseZoomOpt).mapN {
          (changeSource,
           startSequence,
           batchSize,
           tileSource,
           _concurrentUploads,
           databaseUrl,
           baseZoom) =>
            val AppName = "EditHistogramTileUpdater"

            val spark: SparkSession = Analytics.sparkSession(AppName)
            import spark.implicits._
            implicit val concurrentUploads: Option[Int] = _concurrentUploads
            spark.withJTS

            val changeOptions = Map(Source.BaseURI -> changeSource.toString,
                                    Source.ProcessName -> AppName) ++
              databaseUrl
                .map(x => Map(Source.DatabaseURI -> x.toString))
                .getOrElse(Map.empty) ++
              startSequence
                .map(x => Map(Source.StartSequence -> x.toString))
                .getOrElse(Map.empty) ++
              batchSize
                .map(x => Map(Source.BatchSize -> x.toString))
                .getOrElse(Map.empty)

            val changes = spark.readStream
              .format(Source.Changes)
              .options(changeOptions)
              .load

            val changedNodes = changes
              .where('type === "node" and 'lat.isNotNull and 'lon.isNotNull)
              .select('sequence,
                      year('timestamp) * 1000 + dayofyear('timestamp) as 'key,
                      st_makePoint('lon, 'lat) as 'geom)

            val tiledNodes = EditHistogram.update(changedNodes,
                                                  tileSource,
                                                  baseZoom.getOrElse(EditHistogram.DefaultBaseZoom))

            val query = tiledNodes.writeStream
              .queryName("edit histogram tiles")
              .format("console")
              .start

            query.awaitTermination()

            spark.stop()
        }
      }
    )
