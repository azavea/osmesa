package osmesa

import com.monovore.decline._
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql._
import osmesa.ProcessOSM.MultiPolygonRoles
import osmesa.functions._
import osmesa.functions.osm.collectRelation

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

object DebugRelations extends CommandApp(
  name = "osmesa-make-geometries",
  header = "Create geometries from an ORC file",
  main = {

    /* CLI option handling */
    val orcO = Opts.option[String]("orc", help = "Location of the ORC file to process")

    orcO.map { orc =>
      /* Settings compatible for both local and EMR execution */
      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("make-geometries")
        .set("spark.serializer", classOf[org.apache.spark.serializer.KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)
//        .set("spark.sql.orc.impl", "native")

      implicit val ss: SparkSession = SparkSession.builder
        .config(conf)
        .enableHiveSupport
        .getOrCreate

      import ss.implicits._

      /* Silence the damn INFO logger */
      Logger.getRootLogger.setLevel(Level.WARN)

      ss.read.orc(orc)
        .where('id === 8650)
        .where('role.isin(MultiPolygonRoles: _*))
        .distinct
        .repartition(1)
        .groupBy('changeset, 'id, 'version, 'updated)
        .agg(collectRelation('id, 'version, 'updated, 'type, 'role, 'geom) as 'geom)
        .select('changeset, 'id, 'version, 'updated, ST_AsText('geom) as 'wkt)
        .write
        .mode("overwrite")
        .format("csv")
        .save("/tmp/big-geometries-2")

      ss.stop()

      println("Done.")
    }
  }
)
