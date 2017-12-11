package osmesa.analytics.oneoffs

import osmesa.analytics._

import cats.implicits._
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

import java.math.BigDecimal
import java.io.{ByteArrayInputStream}
import java.nio.charset.StandardCharsets
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.{Date, Locale, TimeZone}
import scala.collection.mutable

case class FootprintInfo(user: Long, ageInDays: Int, density: Int)

object FootprintByUserCommand extends CommandApp(

  name   = "footprint-by-user",
  header = "Create footprint vector tiles by user",
  main   = {

    val historyO = Opts.option[String]("history", help = "Location of the History ORC file to process.")
    val changesetsO = Opts.option[String]("changesets", help = "Location of the Changesets ORC file to process.")
    val bucketO = Opts.option[String]("bucket", help = "Bucket to write results to")
    val prefixO = Opts.option[String]("prefix", help = "Prefix of keys path for results.")
    val hashtagsO = Opts.option[String]("hashtags", help = "Path to s3 file containg hashtags to consider.").orNone
    val partitionsO = Opts.option[Int]("partitions", help = "Number of partitions for the partitioner.").orNone
    val publicO = Opts.flag("public", help = "If flag is set, save as public data.").orFalse

    (
      historyO,
      changesetsO,
      bucketO,
      prefixO,
      hashtagsO,
      partitionsO,
      publicO
    ).mapN { (historyUri, changesetsUri, bucket, prefix, hashtagsOpt, partitionsOpt, publicAcl) =>
      FootprintByUser.run(historyUri, changesetsUri, bucket, prefix, hashtagsOpt, partitionsOpt, publicAcl)
    }
  }
)


object FootprintByUser {
  val BASE_ZOOM = 15
  val LAYER_NAME = "user_footprint"

  type FeatureType = ((Double, Double), Long, Int, Int)

  def run(
    historyUri: String,
    changesetsUri: String,
    bucket: String,
    prefix: String,
    hashtagsOpt: Option[String],
    partitionsOpt: Option[Int],
    publicAcl: Boolean
  ): Unit = {
    implicit val spark = Analytics.sparkSession("Footprint By User")
    import spark.implicits._

    try {

      val partitionCount = partitionsOpt.getOrElse(10000)
      val partitioner = new HashPartitioner(partitionCount)

      val history =
        hashtagsOpt match {
          case Some(uri) =>
            val targetHashtags = S3Utils.readText(uri).split("\n").filter(!_.isEmpty).map(_.trim).toSet
            val hist = spark.read.orc(historyUri)
            val changesets = spark.read.orc(changesetsUri)

            val targetUsers =
              changesets.
                where(containsHashtags($"tags", targetHashtags)).
                withColumn("hashtags", hashtags($"tags")).
                where(size($"hashtags") > 0).
                select($"id", $"uid".as("target_uid"), explode($"hashtags").alias("hashtag")).
                drop($"hashtag").
                drop($"id").
                distinct()

            hist.
              where("type = 'node'").
              join(targetUsers, hist("uid") <=> targetUsers("target_uid")).
              drop($"target_uid")
          case None =>
            spark.read.orc(historyUri).
              where("type = 'node'")
        }

      val changesetFootprints =
        history.
          select("lat", "lon", "changeset", "timestamp", "uid").
          repartition(partitionCount).
          map { row =>
            val lat = Option(row.getAs[BigDecimal]("lat")).map(_.doubleValue).getOrElse(Double.NaN)
            val lon = Option(row.getAs[BigDecimal]("lon")).map(_.doubleValue).getOrElse(Double.NaN)
            val ts = row.getAs[java.sql.Timestamp]("timestamp")
            val ageInDays = (((new Date).getTime()-ts.getTime())/(1000*60*60*24)).toInt
            val timestamp = getISO8601StringForDate(ts)
            val changeset = row.getAs[Long]("changeset")
            val user = row.getAs[Long]("uid")

            (user, (lon, lat, ageInDays))
          }.
          rdd.
          filter { case (_, (lon, lat, _)) => isData(lon) && isData(lat) }.
          partitionBy(partitioner).
          map { case (user, (lon, lat, ageInDays)) =>
            Feature(
              Point(lon, lat).reproject(LatLng, WebMercator),
              FootprintInfo(user, ageInDays, 1)
            )
          }

      val baseLayout = ZoomedLayoutScheme(WebMercator).levelForZoom(BASE_ZOOM).layout

      val keyedChangesetFootprints: RDD[((SpatialKey, Long), Iterable[FeatureType])] =
        changesetFootprints.
          clipToGrid(baseLayout).
          map { case (spatialKey, feature) =>
            val coord = feature.geom.jtsGeom.getCoordinate
            (
              (spatialKey, feature.data.user),
              ((spatialKey, ((coord.x, coord.y), feature.data.user, feature.data.ageInDays, 1)))
            )
          }.
          aggregateByKey(Map[(Int, Int), ((Double, Double), Int, Int)](), partitioner)(
            { (acc: Map[(Int, Int), ((Double, Double), Int, Int)], skft: (SpatialKey, FeatureType)) =>
              val (key, (coord, user, ageInDays, density)) = skft
              val extent = baseLayout.mapTransform(key)
              val rasterExtent = RasterExtent(extent, 256, 256)
              val (col, row) = rasterExtent.mapToGrid(coord._1, coord._2)
              acc.get((col, row)) match {
                case Some((existingCoord, existingAge, existingDensity)) =>
                  val (newCoord, newAge) =
                    if(ageInDays < existingAge) {
                      (coord, ageInDays)
                    } else { (existingCoord, existingAge) }
                  acc + (((col, row), (newCoord, newAge, density + existingDensity + 1)))
                case None =>
                  acc + (((col, row),  (coord, ageInDays, density)))
              }

            },
            { (acc1: Map[(Int, Int), ((Double, Double), Int, Int)], acc2: Map[(Int, Int), ((Double, Double), Int, Int)]) =>
              var acc = acc1
              for((key, (coord, ageInDays, density)) <- acc2) {
                acc1.get(key) match {
                  case Some((existingCoord, existingAge, existingDensity)) =>
                    val (newCoord, newAge) =
                      if(ageInDays < existingAge) {
                        (coord, ageInDays)
                      } else { (existingCoord, existingAge) }
                    acc = acc + (key -> (newCoord, newAge, density + existingDensity + 1))
                  case None =>
                    acc = acc + (key -> (coord, ageInDays, density))
                }
              }

              acc
            }
          ).
          mapPartitions({ partition =>
            partition.flatMap { case (key @ (_, user), map) =>
              if(map.isEmpty) { None }
              else {
                Some(
                  (
                    key,
                    map.values.map { case (coord, ageInDays, density) =>
                      (coord, user, ageInDays, density)
                    }
                  )
                )
              }
            }
          }, preservesPartitioning = true)

      var rdd = keyedChangesetFootprints

      for(z <- BASE_ZOOM to 0 by -1) {
        val layout = ZoomedLayoutScheme(WebMercator).levelForZoom(z).layout
        save(z, bakeVectorTiles(rdd, layout), bucket, prefix, publicAcl)
        rdd =
          rdd.
            map { case ((sk, u), geoms) =>
              ((SpatialKey(sk.col/2, sk.row/2), u), geoms)
            }.
            groupByKey(partitioner).
            mapPartitions({ part =>
              part.map { case (key, values) =>
                (key, pyramidUp(values, key._1, layout))
              }
            }, preservesPartitioning = true)
      }
    } finally {
      spark.close()
    }
  }

  def save(zoom: Int, vectorTiles: RDD[((SpatialKey, Long), VectorTile)], bucket: String, prefix: String, publicAcl: Boolean) = {
    val s3PathFromKey: ((SpatialKey, Long)) => String =
      { case (sk, uid) =>
        s"s3://${bucket}/${prefix}/user/${uid}/${zoom}/${sk.col}/${sk.row}.mvt"
      }

    vectorTiles.
      mapValues(_.toBytes).
      saveToS3(s3PathFromKey, putObjectModifier = { o => if(publicAcl) o.withCannedAcl(PublicRead) else o })
  }

  def bakeVectorTiles(
    rdd: RDD[((SpatialKey, Long), Iterable[FeatureType])],
    layout: LayoutDefinition
  ): RDD[((SpatialKey, Long), VectorTile)] =
    rdd.
      mapPartitions({ partition =>
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
              Feature(Point(coord._1, coord._2), vtData)
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

    val results = mutable.Map[(Long, Int, Int), ((Double, Double), Int, Int)]()

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
}
