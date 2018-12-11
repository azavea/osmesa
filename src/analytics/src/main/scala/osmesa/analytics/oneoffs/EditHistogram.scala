package osmesa.analytics.oneoffs

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import osmesa.analytics.{Analytics, EditHistogram}
import org.locationtech.geomesa.spark.jts._

object EditHistogramCommand
    extends CommandApp(
      name = "edit-histogram",
      header = "Create vector tiles containing histograms of editing activity",
      main = {

        val historyOpt = Opts
          .option[URI]("history", help = "URI of the history ORC file to process.")
        val outputOpt = Opts.option[URI]("out", help = "Base URI for output.")

        (
          historyOpt,
          outputOpt
        ).mapN { (historyURI, outputURI) =>
          implicit val spark: SparkSession = Analytics.sparkSession("Edit Histogram")
          import spark.implicits._
          spark.withJTS

          val history = spark.read.orc(historyURI.toString)

          val nodes = history
            .where('type === "node" and 'lat.isNotNull and 'lon.isNotNull)
            .where('uid > 1)
            .select('lat, 'lon, year('timestamp) * 100 + weekofyear('timestamp) as 'key)

          val stats = EditHistogram.createTiles(nodes, outputURI)
          stats.repartition(1).write.mode(SaveMode.Overwrite).csv("/tmp/currency/")

          spark.stop()
        }
      }
    )
