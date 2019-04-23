package osmesa.analytics.oneoffs

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.locationtech.geomesa.spark.jts._
import osmesa.analytics.{Analytics, EditHistogram}

object EditHistogramTileCreator
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
          implicit val spark: SparkSession = Analytics.sparkSession("State of the Data tile generation")
          import spark.implicits._
          spark.withJTS

          val history = spark.read.orc(historyURI.toString)

          val nodes = history
            .where('type === "node" and 'lat.isNotNull and 'lon.isNotNull)
            .withColumn("lat", 'lat.cast(DoubleType))
            .withColumn("lon", 'lon.cast(DoubleType))
            .where('uid > 1)
            .select('lat, 'lon, year('timestamp) * 1000 + dayofyear('timestamp) as 'key)

          val stats = EditHistogram.create(nodes, outputURI)
          stats.show

          spark.stop()
        }
      }
    )
