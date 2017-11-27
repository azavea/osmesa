package osmesa.analytics.oneoffs

import osmesa.analytics._

import com.amazonaws.services.s3.model.{PutObjectRequest, ObjectMetadata}
import com.amazonaws.services.s3.model.CannedAccessControlList._
import com.monovore.decline._
import com.vividsolutions.jts.{geom => jts}
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.io.s3._
import geotrellis.util._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vectortile._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import spray.json.DefaultJsonProtocol._
import vectorpipe._

import java.io.{ByteArrayInputStream}
import java.nio.charset.StandardCharsets
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.{Date, Locale, TimeZone}
import scala.collection.mutable

case class FootprintInfo(user: Long, ageInDays: Int, density: Int)

object FootprintByUser {
  val BASE_ZOOM = 15
  val LAYER_NAME = "user_footprint"

  type FeatureType = (jts.Coordinate, Long, Int, Int)

  def save(zoom: Int, vectorTiles: RDD[((SpatialKey, Int), VectorTile)]) = {
    val s3PathFromKey: ((SpatialKey, Int)) => String =
      { case (sk, name) =>
        // val n =  URLEncoder.encode(name, "UTF-8")
        val n =  name
        s"s3://osmesa-osm-pds/test/results/user-extent-vts/${n}/${zoom}/${sk.col}/${sk.row}.mvt"
      }

    vectorTiles
      .mapValues(_.toBytes)
      .saveToS3(s3PathFromKey, putObjectModifier = { o => o.withCannedAcl(PublicRead)})
  }

  def bakeVectorTiles(
    rdd: RDD[((SpatialKey, Int), Iterable[FeatureType])],
    layout: LayoutDefinition
  ): RDD[((SpatialKey, Int), VectorTile)] =
    rdd
      .mapPartitions({ partition =>
        val mapTransform = layout.mapTransform
        partition.map { case ((spatialKey, user), pointFeatures) =>
          val tileExtent = mapTransform(spatialKey)
          val vtFeatures =
            pointFeatures.map { case (coord, user, ageInDays, density) =>
              val vtData =
                Map(
                  "user" -> VInt64(user),
                  "ageInDays" -> VInt64(ageInDays),
                  "density" -> VInt64(density)
                )
              Feature(Point(coord.x, coord.y), vtData)
            }
          val layer =
            StrictLayer(
                name=LAYER_NAME,
                tileWidth=4096,
                version=2,
                tileExtent=tileExtent,
                points=vtFeatures.toSeq,
                multiPoints=Seq[Feature[MultiPoint, Map[String, Value]]](),
                lines=Seq[Feature[Line, Map[String, Value]]](),
                multiLines=Seq[Feature[MultiLine, Map[String, Value]]](),
                polygons=Seq[Feature[Polygon, Map[String, Value]]](),
                multiPolygons=Seq[Feature[MultiPolygon, Map[String, Value]]]()
              )

          val vt = VectorTile(Map(LAYER_NAME -> layer), tileExtent)

         ((spatialKey, user), vt)
        }
      }, preservesPartitioning = true)

  def getISO8601StringForDate(date: Date): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US)
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    dateFormat.format(date)
  }

  def pyramidUp(
    features: Iterable[Iterable[FeatureType]],
    key: SpatialKey,
    layout: LayoutDefinition
  ): Iterable[FeatureType] = {
    val extent = layout.mapTransform(key)
    val rasterExtent = RasterExtent(extent, 256, 256)

    // Strategy here is to only allow one point per rasterExtent cell.
    // Merge points by taking latest timestamp and youngest age, and
    // increasing density of the representative point.

    val results = mutable.Map[(Long, Int, Int), (jts.Coordinate, Int, Int)]()

    for(part <- features;
        thisFeature @ (coord, user, ageInDays, density) <- part) {
      val (col, row) = rasterExtent.mapToGrid(coord.x, coord.y)
      results.get((user, col, row)) match {
        case Some((existingCoord, existingAge, existingDensity)) =>
          val (newCoord, newAge) =
            if(ageInDays < existingAge) {
              (coord, ageInDays)
            } else { (existingCoord, existingAge) }
          results((user, col, row)) = (newCoord, newAge, density + existingDensity + 1)
        case None =>
            results((user, col, row)) = (coord, ageInDays, density)
      }
    }

    results.map { case ((user, _, _), (coord, ageInDays, density)) => (coord, user, ageInDays, density) }
  }

  val bigBadUsers =
    List(
      "DaveHansenTiger",
      "woodpeck_fixbot",
      "woodpeck_repair",
      "OSMF Redaction Account"
    )

  def main(args: Array[String]): Unit = {
    implicit val ss = Analytics.sparkSession("Footprint By User")
    import ss.implicits._

    val partitioner = new HashPartitioner(10000)

    val history = OSMOrc.planetHistory

    val usersToProcess =
      OSMOrc.planetHistory
        .select("timestamp", "user", "uid")
        .where("type = 'node'")
        .filter(!$"user".isin(bigBadUsers: _*))
        .groupBy("user", "uid")
        .count()
        .where("count >= 100")

    usersToProcess
      .coalesce(1)
      .write
      .format("csv")
      .save("s3://osmesa-osm-pds/test/results/users.csv")


    val idsToProcess =
      usersToProcess
        .rdd
        .map { row => row.getAs[Long]("uid") }
        .collect
        .toSet

    val changesetFootprints =
      history
        .select("lat", "lon", "changeset", "timestamp", "uid")
        .where("type = 'node'")
        .repartition(10000)
        .map { row =>
          val lat = row.getAs[java.math.BigDecimal]("lat").doubleValue()
          val lon = row.getAs[java.math.BigDecimal]("lon").doubleValue()
          val ts = row.getAs[java.sql.Timestamp]("timestamp")
          val ageInDays = (((new Date).getTime()-ts.getTime())/(1000*60*60*24)).toInt
          val timestamp = getISO8601StringForDate(ts)
          val changeset = row.getAs[Long]("changeset")
          val user = row.getAs[Long]("uid")

          (user, lon, lat, ageInDays)
        }
        .rdd
        .flatMap { case (user, lon, lat, ageInDays) =>
          if(idsToProcess.contains(user)) {
            if(isData(lon) && isData(lat) && Extent(-180, -89.99999, 179.99999, 89.99999).contains(lon, lat)) {
              Some(
                Feature(
                  Point(lon, lat).reproject(LatLng, WebMercator),
                  FootprintInfo(user, ageInDays, 1)
                )
              )
            } else { None }
          } else { None }
        }


    val baseLayout = ZoomedLayoutScheme(WebMercator).levelForZoom(BASE_ZOOM).layout

    val keyedChangesetFootprints: RDD[((SpatialKey, Int), Iterable[FeatureType])] =
      changesetFootprints
        .clipToGrid(baseLayout)
        .map { case (spatialKey, feature) =>
          val userKey = (feature.data.user % 100).toInt
          (
            (spatialKey, userKey),
            (feature.geom.jtsGeom.getCoordinate, feature.data.user, feature.data.ageInDays, 1)
          )
        }
        .groupByKey(partitioner)

    var rdd = keyedChangesetFootprints
    for(z <- BASE_ZOOM to 0 by -1) {
      val layout = ZoomedLayoutScheme(WebMercator).levelForZoom(z).layout
      save(z, bakeVectorTiles(rdd, layout))
      rdd =
        rdd
          .map { case ((sk, u), geoms) =>
            ((SpatialKey(sk.col/2, sk.row/2), u), geoms)
          }
          .groupByKey(partitioner)
          .mapPartitions({ part =>
            part.map { case (key, values) =>
              (key, pyramidUp(values, key._1, layout))
            }
          }, preservesPartitioning = true)
    }
  }
}
