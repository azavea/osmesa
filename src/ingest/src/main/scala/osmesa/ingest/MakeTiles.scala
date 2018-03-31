package osmesa

import cats.implicits._
import com.monovore.decline._
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.{LayoutLevel, ZoomedLayoutScheme}
import geotrellis.vector.{Feature, Geometry}
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import geotrellis.vector.io._
import geotrellis.vectortile.{VInt64, VString}

/*
 * Usage example:
 *
 * sbt "project ingest" assembly
 *
 * spark-submit \
 *   --class osmesa.MakeTiles \
 *   ingest/target/scala-2.11/osmesa-ingest.jar \
 *   --orc=$HOME/data/osm/isle-of-man-geoms.orc \
 *   --changesets=$HOME/data/osm/isle-of-man-changesets.orc \
 *   --bucket=s3-bucket \
 *   --key=isle-of-man
 */

object MakeTiles extends CommandApp(
  name = "osmesa-make-tiles",
  header = "Create tiles from an ORC file",
  main = {

    /* CLI option handling */
    val orcO = Opts.option[String]("orc", help = "Location of the ORC file containing geometries to process")
    val changesetsO = Opts.option[String]("changesets", help = "Location of the ORC file containing changesets")
    val bucketO = Opts.option[String]("bucket", help = "S3 bucket to write VTs to")
    val prefixO = Opts.option[String]("key", help = "S3 directory (in bucket) to write to")

    (orcO, changesetsO, bucketO, prefixO).mapN { (orc, changesetsSrc, bucket, prefix) =>
      /* Settings compatible for both local and EMR execution */
      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("make-tiles")
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

      val changesets = ss.read.orc(changesetsSrc)
      val geoms = ss.read.orc(orc)
        .join(changesets.select('id as 'changeset, 'uid, 'user), Seq("changeset"))

      val features: RDD[GenerateVT.VTF[Geometry]] = geoms
        .rdd
        .flatMap { row =>
          val id = row.getAs[Long]("id")
          val geom = row.getAs[scala.Array[Byte]]("geom")
          val tags = row.getAs[Map[String, String]]("tags")
          val changeset = row.getAs[Long]("changeset")
          val updated = row.getAs[java.sql.Timestamp]("updated")
          val validUntil = row.getAs[java.sql.Timestamp]("validUntil")
          val version = row.getAs[Long]("version")
          val minorVersion = row.getAs[Int]("minorVersion")
          val uid = row.getAs[Long]("uid")
          val user = row.getAs[String]("user")

          // check validity of reprojected geometry
          Option(geom).map(_.readWKB.reproject(LatLng, WebMercator)) match {
            case Some(g) if g.isValid =>
              Seq(Feature(
                g,
                tags.map {
                  case (k, v) => (k, VString(v))
                } ++ Map(
                  "__id" -> VInt64(id),
                  "__changeset" -> VInt64(changeset),
                  "__updated" -> VInt64(updated.getTime),
                  "__validUntil" -> VInt64(Option(validUntil).map(_.getTime).getOrElse(0)),
                  "__version" -> VInt64(version),
                  "__minorVersion" -> VInt64(minorVersion),
                  "__uid" -> VInt64(uid),
                  "__user" -> VString(user)
                )
              ))
            case _ => Seq()
          }
        }

      val layoutScheme = ZoomedLayoutScheme(WebMercator, 512)

      def build[G <: Geometry](keyedGeoms: RDD[(SpatialKey, (SpatialKey, GenerateVT.VTF[G]))], layoutLevel: LayoutLevel): Unit = {
        val LayoutLevel(zoom, layout) = layoutLevel

        GenerateVT.save(GenerateVT.makeVectorTiles(keyedGeoms, layout, "all"), zoom, bucket, prefix)
      }

      val maxLayoutLevel = layoutScheme.levelForZoom(13)
      val keyed = GenerateVT.keyToLayout(features, maxLayoutLevel.layout)

      build(keyed, maxLayoutLevel)

      ss.stop()

      println("Done.")
    }
  }
)