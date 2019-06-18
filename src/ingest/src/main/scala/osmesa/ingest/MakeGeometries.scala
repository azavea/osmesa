package osmesa

import cats.implicits._
import com.monovore.decline._
import geotrellis.spark.io.kryo.KryoRegistrator
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.locationtech.geomesa.spark.jts._
import osmesa.common.ProcessOSM
import osmesa.common.functions.box2d

/*
 * Usage example:
 *
 * sbt "project ingest" assembly
 *
 * spark-submit \
 *   --class osmesa.MakeGeometries \
 *   ingest/target/scala-2.11/osmesa-ingest.jar \
 *   --orc=$HOME/data/osm/isle-of-man.orc \
 *   --out=$HOME/data/osm/isle-of-man-geoms/ \
 */

object MakeGeometries
    extends CommandApp(
      name = "osmesa-make-geometries",
      header = "Create geometries from an ORC file",
      main = {

        /* CLI option handling */
        val orcO = Opts.option[String]("orc", help = "Location of the ORC file to process")
        val outO = Opts.option[String]("out", help = "ORC file containing geometries")
        val numPartitionsO =
          Opts.option[Int]("partitions", help = "Number of partitions to generate").withDefault(1)

        (orcO, outO, numPartitionsO).mapN {
          (orc, out, numPartitions) =>
            /* Settings compatible for both local and EMR execution */
            val conf = new SparkConf()
              .setIfMissing("spark.master", "local[*]")
              .setAppName("make-geometries")
              .set("spark.serializer", classOf[KryoSerializer].getName)
              .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
              .set("spark.sql.orc.impl", "native")
              .set("spark.sql.orc.filterPushdown", "true")
              .set("spark.ui.showConsoleProgress", "true")

            implicit val spark: SparkSession = SparkSession.builder
              .config(conf)
              .getOrCreate
            import spark.implicits._

            Logger.getRootLogger.setLevel(Level.WARN)

            val df = spark.read.orc(orc)

            ProcessOSM
              .constructGeometries(df)
              // create a bbox struct to optimize spatial filtering
              .withColumn("bbox", box2d('geom))
              // store the geometry as WKB
              .withColumn("geom", when(not(st_isEmpty('geom)), st_asBinary('geom)))
              .repartition(numPartitions)
              .write
              .mode(SaveMode.Overwrite)
              .orc(out)

            spark.stop()

            println("Done.")
        }
      }
    )
