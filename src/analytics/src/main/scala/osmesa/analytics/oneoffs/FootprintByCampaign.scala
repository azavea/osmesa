package osmesa.analytics.oneoffs

import java.math.BigDecimal
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.{Date, Locale, TimeZone}

import cats.implicits._
import com.amazonaws.services.s3.model.CannedAccessControlList._
import com.monovore.decline._
import com.vividsolutions.jts.{geom => jts}
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io.s3._
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.vectortile._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import osmesa.analytics._

import scala.collection.mutable

case class HashtagFootprintInfo(hashtag: String, ageInDays: Int, density: Int)

object FootprintByCampaignCommand extends CommandApp(

  name   = "footprint-by-campaign",
  header = "Create footprint vector tiles by campaign",
  main   = {

    val historyO = Opts.option[String]("history", help = "Location of the History ORC file to process.")
    val changesetsO = Opts.option[String]("changesets", help = "Location of the Changesets ORC file to process.")
    val bucketO = Opts.option[String]("bucket", help = "Bucket to write results to")
    val prefixO = Opts.option[String]("prefix", help = "Prefix of keys path for results.")
    val hashtagsO = Opts.option[String]("hashtags", help = "Path to s3 file containing hashtags to consider.").orNone
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
      FootprintByCampaign.run(historyUri, changesetsUri, bucket, prefix, hashtagsOpt, partitionsOpt, publicAcl)
    }
  }
)

object FootprintByCampaign {
  val BASE_ZOOM = 15
  val LAYER_NAME = "hashtag_footprint"

  type FeatureType = (jts.Coordinate, String, Int, Int)

  def run(
    historyUri: String,
    changesetsUri: String,
    bucket: String,
    prefix: String,
    hashtagsOpt: Option[String],
    partitionsOpt: Option[Int],
    publicAcl: Boolean
  ): Unit = {
    implicit val spark = Analytics.sparkSession("Footprint By Hashtag")
    import spark.implicits._

    try {
      val partitioner = new HashPartitioner(partitionsOpt.getOrElse(10000))
      val targetHashtags =
        hashtagsOpt.map { uri =>
          S3Utils.readText(uri).split("\n").map(_.toLowerCase).toSet
        }

      val history = spark.read.orc(historyUri)
      val changesets = spark.read.orc(changesetsUri)

      val historyNodes =
        history.
          select("lat", "lon", "changeset", "timestamp").
          where("type = 'node'")

      val changesetToHashtag =
        changesets.
          withColumn("hashtags", hashtags($"tags")).
          where(size($"hashtags") > 0).
          select($"id", explode($"hashtags").alias("hashtag")).
          map { row =>
            (row.getAs[Long]("id"), row.getAs[String]("hashtag"))
          }

      // Find hashtag -> changeset count
      // changesetToHashtag.groupBy("hashtag").collect()

      val filteredChangesetToHashtag =
        targetHashtags match {
          case Some(hashtags) =>
            changesetToHashtag.
              rdd.
              filter { case (_, hashtag) =>
                hashtags.contains(hashtag)
              }.
              partitionBy(partitioner)
          case None =>
            changesetToHashtag.
              rdd.
              partitionBy(partitioner)
        }

      val changesetToHistoryNodes =
        historyNodes.
          map { row =>
            val lat = Option(row.getAs[BigDecimal]("lat")).map(_.doubleValue).getOrElse(Double.NaN)
            val lon = Option(row.getAs[BigDecimal]("lon")).map(_.doubleValue).getOrElse(Double.NaN)
            val ts = row.getAs[java.sql.Timestamp]("timestamp")
            val ageInDays = (((new Date).getTime()-ts.getTime())/(1000*60*60*24)).toInt
            val changeset = row.getAs[Long]("changeset")

            (changeset, (lon, lat, ageInDays))
          }.
          rdd.
          filter { case (_, (lon, lat, _)) => isData(lon) && isData(lat) }.
          partitionBy(partitioner)

      val features: RDD[PointFeature[HashtagFootprintInfo]] =
        filteredChangesetToHashtag.
          join(changesetToHistoryNodes).
          map { case (_, (hashtag, (lon, lat, ageInDays))) =>
            Feature(
              Point(lon, lat).reproject(LatLng, WebMercator),
              HashtagFootprintInfo(hashtag, ageInDays, 1)
            )
          }

      val baseLayout = ZoomedLayoutScheme(WebMercator).levelForZoom(BASE_ZOOM).layout

      val keyedChangesetFootprints: RDD[((SpatialKey, String), Iterable[FeatureType])] =
        features.
          clipToGrid(baseLayout).
          map { case (spatialKey, feature) =>
            (
              (spatialKey, feature.data.hashtag),
              ((spatialKey, (feature.geom.jtsGeom.getCoordinate, feature.data.hashtag, feature.data.ageInDays, 1)))
            )
          }.
          aggregateByKey(Map[(Int, Int), (jts.Coordinate, Int, Int)](), partitioner)(
            { (acc: Map[(Int, Int), (jts.Coordinate, Int, Int)], skft: (SpatialKey, FeatureType)) =>
              val (key, (coord, hashtag, ageInDays, density)) = skft
              val extent = baseLayout.mapTransform(key)
              val rasterExtent = RasterExtent(extent, 256, 256)
              val (col, row) = rasterExtent.mapToGrid(coord.x, coord.y)
              acc.get((col, row)) match {
                case Some((existingCoord, existingAge, existingDensity)) =>
                  val (newCoord, newAge) =
                    if(ageInDays < existingAge) {
                      (coord, ageInDays)
                    } else { (existingCoord, existingAge) }
                  acc + ((col, row) -> (newCoord, newAge, density + existingDensity + 1))
                case None =>
                  acc + ((col, row) -> (coord, ageInDays, density))
              }

            },
            { (acc1: Map[(Int, Int), (jts.Coordinate, Int, Int)], acc2: Map[(Int, Int), (jts.Coordinate, Int, Int)]) =>
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
            partition.map { case (key @ (_, hashtag), map) =>
              (
                key,
                map.values.map { case (coord, ageInDays, density) =>
                  (coord, hashtag, ageInDays, density)
                }
              )
            }
          }, preservesPartitioning = true)

      var rdd = keyedChangesetFootprints
      for(z <- BASE_ZOOM to 0 by -1) {
        val layout = ZoomedLayoutScheme(WebMercator).levelForZoom(z).layout
        save(z, bakeVectorTiles(rdd, layout), bucket, prefix, publicAcl)
        rdd =
          rdd.
            map { case ((sk, h), geoms) =>
              ((SpatialKey(sk.col/2, sk.row/2), h), geoms)
            }.
            groupByKey(partitioner).
            mapPartitions({ part =>
              part.map { case (key, values) =>
                (key, pyramidUp(values, key._1, layout))
              }
            }, preservesPartitioning = true)
      }
    } finally {
      spark.stop()
    }
  }

  def save(zoom: Int, vectorTiles: RDD[((SpatialKey, String), VectorTile)], bucket: String, prefix: String, publicAcl: Boolean) = {
    val s3PathFromKey: ((SpatialKey, String)) => String =
      { case (sk, name) =>
        val n =  URLEncoder.encode(name, "UTF-8")
        s"s3://${bucket}/${prefix}/hashtag/${n}/${zoom}/${sk.col}/${sk.row}.mvt"
      }

    vectorTiles.
      mapValues(_.toBytes).
      saveToS3(s3PathFromKey, putObjectModifier = { o => if(publicAcl) o.withCannedAcl(PublicRead) else o })
  }

  def bakeVectorTiles(
    rdd: RDD[((SpatialKey, String), Iterable[FeatureType])],
    layout: LayoutDefinition
  ): RDD[((SpatialKey, String), VectorTile)] =
    rdd.
      mapPartitions({ partition =>
        val mapTransform = layout.mapTransform
        partition.map { case ((spatialKey, hashtag), pointFeatures) =>
          val tileExtent = mapTransform(spatialKey)
          val vtFeatures =
            pointFeatures.map { case (coord, hashtag, ageInDays, density) =>
              val vtData =
                Map(
                  "hashtag" -> VString(hashtag),
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

         ((spatialKey, hashtag), vt)
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

    val results = mutable.Map[(String, Int, Int), (jts.Coordinate, Int, Int)]()

    for(part <- features;
        thisFeature @ (coord, hashtag, ageInDays, density) <- part) {
      val (col, row) = rasterExtent.mapToGrid(coord.x, coord.y)
      results.get((hashtag, col, row)) match {
        case Some((existingCoord, existingAge, existingDensity)) =>
          val (newCoord, newAge) =
            if(ageInDays < existingAge) {
              (coord, ageInDays)
            } else { (existingCoord, existingAge) }
          results((hashtag, col, row)) = (newCoord, newAge, density + existingDensity + 1)
        case None =>
            results((hashtag, col, row)) = (coord, ageInDays, density)
      }
    }

    results.map { case ((hashtag, _, _), (coord, ageInDays, density)) => (coord, hashtag, ageInDays, density) }
  }

}
