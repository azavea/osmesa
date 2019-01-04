package osmesa

import java.sql.Timestamp

import com.monovore.decline._
import com.vividsolutions.jts.{geom => jts}
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.IntegerType
import org.locationtech.geomesa.spark.jts.st_asText
import osmesa.common.ProcessOSM._
import osmesa.common.functions.osm._
import osmesa.common.relations.MultiPolygons.build

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

      implicit val encoder: Encoder[Row] = VersionedElementEncoder

      ss.read.orc(orc)
        .where('id === 8650)
        .where('role.isin(MultiPolygonRoles: _*))
        .withColumn("version", 'version.cast(IntegerType))
        .distinct
        .repartition('changeset, 'id, 'version, 'minorVersion, 'updated, 'validUntil)
        .mapPartitions(rows => {
          rows
            .toVector
            .groupBy(row =>
              (row.getAs[Long]("changeset"), row.getAs[Long]("id"), row.getAs[Integer]("version"), row.getAs[Integer]("minorVersion"), row.getAs[Timestamp]("updated"), row.getAs[Timestamp]("validUntil"))
            )
            .map {
              case ((changeset, id, version, minorVersion, updated, validUntil), rows: Seq[Row]) =>
                val types = rows.map(_.getAs[String]("type") match {
                  case "node" => NodeType
                  case "way" => WayType
                  case "relation" => RelationType
                  case _ => null.asInstanceOf[Byte]
                })
                val roles = rows.map(_.getAs[String]("role"))
                val geoms = rows.map(_.getAs[jts.Geometry]("geom"))

                val wkb = build(id, version, updated, types, roles, geoms).orNull

                new GenericRowWithSchema(Array(changeset, id, version, minorVersion, updated, validUntil, wkb), VersionedElementSchema): Row
            }
            .toIterator
        })
        .select('changeset, 'id, 'version, 'updated, st_asText('geom) as 'wkt)
        .repartition(1)
        .write
        .mode("overwrite")
        .format("csv")
        .save("/tmp/big-geometries-2")

      ss.stop()

      println("Done.")
    }
  }
)
