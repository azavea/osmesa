package osmesa

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import osmesa.ingest.util.Caching
import osmesa.functions._

/*
 * Usage example:
 *
 * sbt "project ingest" assembly
 *
 * spark-submit \
 *   --class osmesa.MakeGeometries \
 *   ingest/target/scala-2.11/osmesa-ingest.jar \
 *   --orc=$HOME/data/osm/isle-of-man.orc \
 *   --out=$HOME/data/osm/isle-of-man-geoms.orc \
 */

object MakeGeometries extends CommandApp(
  name = "osmesa-make-geometries",
  header = "Create geometries from an ORC file",
  main = {

    /* CLI option handling */
    val orcO = Opts.option[String]("orc", help = "Location of the ORC file to process")
    val outO = Opts.option[String]("out", help = "ORC file containing geometries")
    val numPartitionsO = Opts.option[Int]("partitions", help = "Number of partitions to generate").withDefault(1)
    val cacheDirO = Opts.option[String]("cache", help = "Location to cache ORC files").withDefault("")

    (orcO, outO, numPartitionsO, cacheDirO).mapN { (orc, out, numPartitions, cacheDir) =>
      /* Settings compatible for both local and EMR execution */
      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("make-geometries")
        .set("spark.serializer", classOf[org.apache.spark.serializer.KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)
        .set("spark.sql.orc.impl", "native")

      implicit val ss: SparkSession = SparkSession.builder
        .config(conf)
        .enableHiveSupport
        .getOrCreate

      import ss.implicits._

      /* Silence the damn INFO logger */
      Logger.getRootLogger.setLevel(Level.WARN)

      val df = ss.read.orc(orc)

      val cache = Option(new URI(cacheDir).getScheme) match {
        case Some("s3") => Caching.onS3(cacheDir)
        // bare paths don't get a scheme
        case None if cacheDir != "" => Caching.onFs(cacheDir)
        case _ => Caching.none
      }

      val nodes = cache.orc("prepared_nodes") {
        ProcessOSM.preprocessNodes(df)
      }

      val nodeGeoms = cache.orc("node_geoms") {
        ProcessOSM.constructPointGeometries(nodes)
      }.withColumn("minorVersion", lit(0))

      val wayGeoms = cache.orc("way_geoms") {
        ProcessOSM.reconstructWayGeometries(df, nodes)
      }

      val relationGeoms = cache.orc("relation_geoms") {
        ProcessOSM.reconstructRelationGeometries(df, wayGeoms)
      }

      // TODO remove untagged way geoms and ones that contribute to ways but have no inherent value
      nodeGeoms
        .union(wayGeoms)
        .union(relationGeoms)
        .withColumn("wkt", ST_AsText('geom))
        .drop('geom)
        .drop('tags)
        .repartition(numPartitions)
        .write
        .orc(out)

      ss.stop()

      println("Done.")
    }
  }
)
