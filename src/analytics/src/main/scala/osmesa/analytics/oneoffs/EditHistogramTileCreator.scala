package osmesa.analytics.oneoffs

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.locationtech.geomesa.spark.jts._
import osmesa.analytics.{Analytics, EditHistogram}
import vectorpipe.functions.asDouble

object EditHistogramTileCreator
    extends CommandApp(
      name = "edit-histogram",
      header = "Create vector tiles containing histograms of editing activity",
      main = {

        val historyOpt = Opts
          .option[URI]("history", help = "URI of the history ORC file to process.")

        val outputOpt = Opts.option[URI]("out", help = "Base URI for output.")

        val concurrentUploadsOpt = Opts
          .option[Int]("concurrent-uploads",
                       short = "c",
                       metavar = "concurrent uploads",
                       help = "Set the number of concurrent uploads.")
          .orNone

        val baseZoomOpt = Opts
          .option[Int]("base-zoom",
                       short = "z",
                       metavar = "Base zoom",
                       help = "Most detailed zoom level")
          .orNone

        (
          historyOpt,
          outputOpt,
          concurrentUploadsOpt,
          baseZoomOpt
        ).mapN {
          (historyURI, outputURI, _concurrentUploads, baseZoom) =>
            implicit val spark: SparkSession =
              Analytics.sparkSession("State of the Data tile generation")
            import spark.implicits._
            implicit val concurrentUploads: Option[Int] = _concurrentUploads
            spark.withJTS

            val history = spark.read.orc(historyURI.toString)

            val nodes = history
              .where('type === "node" and 'lat.isNotNull and 'lon.isNotNull)
              .withColumn("lat", asDouble('lat))
              .withColumn("lon", asDouble('lon))
              .where('uid > 1)
              .select(st_makePoint('lon, 'lat) as 'geom,
                      year('timestamp) * 1000 + dayofyear('timestamp) as 'key)

            val stats = EditHistogram.create(nodes,
                                             outputURI,
                                             baseZoom.getOrElse(EditHistogram.DefaultBaseZoom))
            println(s"${stats.count} tiles created.")

            spark.stop()
        }
      }
    )
