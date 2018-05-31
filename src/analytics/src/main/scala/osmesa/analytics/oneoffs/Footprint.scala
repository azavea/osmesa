package osmesa.analytics.oneoffs

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.math.BigDecimal
import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.zip.GZIPOutputStream

import cats.implicits._
import com.amazonaws.services.s3.model.{AmazonS3Exception, ObjectMetadata, PutObjectRequest}
import com.monovore.decline._
import com.vividsolutions.jts.{geom => jts}
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.resample.Sum
import geotrellis.spark._
import geotrellis.spark.io.s3.S3Client
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vectortile.{StrictLayer, VInt64, Value, VectorTile}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import osmesa.analytics.{Analytics, S3Utils}
import osmesa.common.functions.osm._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

object FootprintCommand
    extends CommandApp(
      name = "footprint",
      header = "Create footprint vector tiles",
      main = {

        val historyOpt = Opts
          .option[URI]("history", help = "URI of the history ORC file to process.")
        val changesetsOpt = Opts
          .option[URI]("changesets", help = "URI of the changesets ORC file to process.")
        val hashtagsOpt =
          Opts.option[URI]("include-hashtags", help = "URI containing hashtags to consider.").orNone
        val outputOpt = Opts.option[URI]("out", help = "Base URI for output.")
        val typeOpt =
          Opts.option[String]("type", "Type of footprints to generate (users, hashtags)")

        (
          historyOpt,
          changesetsOpt,
          hashtagsOpt,
          outputOpt,
          typeOpt
        ).mapN { (historyUri, changesetsUri, hashtagsURI, outputURI, footprintType) =>
          Footprint.run(historyUri, changesetsUri, hashtagsURI, outputURI, footprintType)
        }
      }
    )

object Footprint extends Logging {
  // TODO make this configurable
  val BASE_ZOOM = 15
  type KeyedTile = (String, Int, Int, Int, Raster[Tile])

  type FeatureType = (jts.Coordinate, String, Int)
  val LayoutScheme = ZoomedLayoutScheme(WebMercator)
  val TiledGeometrySchema = StructType(
    StructField("key", StringType, nullable = false) ::
      StructField("zoom", IntegerType, nullable = false) ::
      StructField("col", IntegerType, nullable = false) ::
      StructField("row", IntegerType, nullable = false) ::
      StructField("geom", BinaryType, nullable = true) ::
      Nil)

  val TiledGeometryEncoder: Encoder[Row] = RowEncoder(TiledGeometrySchema)

  val Cols = 256
  val Rows = 256

  implicit def encodeTile(tile: Tile): (Array[Byte], Int, Int) =
    (tile.toBytes, tile.cols, tile.rows)
  implicit def decodeTile(tile: (Array[Byte], Int, Int)): Tile =
    IntArrayTile.fromBytes(tile._1, tile._2, tile._3)

  implicit val tupleEncoder: Encoder[KeyedTile] = Encoders.kryo[KeyedTile]
  implicit val encoder: Encoder[Row] = TiledGeometryEncoder

  def downsample(tiles: Dataset[KeyedTile]): Dataset[KeyedTile] = {
    import tiles.sparkSession.implicits._

    tiles.map {
      case (k, z, x, y, Raster(tile, tileExtent)) =>
        val newTile = tile.resample(Cols / 2, Rows / 2, Sum)

        (k, z - 1, x / 2, y / 2, Raster.tupToRaster(newTile, tileExtent))
    } groupByKey {
      case (k, z, x, y, _) => (k, z, x, y)
    } mapGroups {
      case ((k, z, x, y), rows) =>
        val rasters = rows.map(_._5).toList
        val targetExtent = SpatialKey(x, y).extent(LayoutScheme.levelForZoom(z).layout)

        val newTile = rasters.head.tile.prototype(Cols, Rows)

        rasters.foreach { raster =>
          newTile.merge(targetExtent, raster.extent, raster.tile, Sum)
        }

        (k, z, x, y, Raster.tupToRaster(newTile, targetExtent))
    }
  }

  def tile(history: DataFrame, zoom: Int): Dataset[KeyedTile] = {
    import history.sparkSession.implicits._
    val layout = LayoutScheme.levelForZoom(zoom).layout

    history
      .where('type === "node" and 'lat.isNotNull and 'lon.isNotNull)
      .repartition('id, 'version)
      .select('key, 'lat, 'lon)
      .flatMap { row =>
        val key = row.getAs[String]("key")
        val lat = row.getAs[BigDecimal]("lat").floatValue
        val lon = row.getAs[BigDecimal]("lon").floatValue

        val geom = Point(lon, lat)

        Option(geom).map(_.reproject(LatLng, WebMercator)) match {
          case Some(g) if g.isValid =>
            layout.mapTransform
              .keysForGeometry(g)
              .flatMap { sk =>
                g.intersection(sk.extent(layout)).toGeometry match {
                  case Some(clipped) if clipped.isValid =>
                    Seq(
                      new GenericRowWithSchema(
                        Array(key, zoom, sk.col, sk.row, clipped.toWKB(3857)),
                        TiledGeometrySchema): Row)
                  case _ => Seq.empty[Row]
                }
              }
          case _ => Seq.empty[Row]
        }
      } groupByKey { row =>
      (row.getAs[String]("key"),
       row.getAs[Int]("zoom"),
       row.getAs[Int]("col"),
       row.getAs[Int]("row"))
    } mapGroups {
      case ((k, z, x, y), rows) =>
        val sk = SpatialKey(x, y)
        val tileExtent = sk.extent(layout)
        val tile = IntArrayTile.ofDim(Cols * 4, Rows * 4)
        val rasterExtent = RasterExtent(tileExtent, tile.cols, tile.rows)
        val geoms = rows.map(_.getAs[Array[Byte]]("geom").readWKB)

        geoms.foreach(g =>
          g.foreach(rasterExtent) { (c, r) =>
            tile.set(c, r, tile.get(c, r) + 1)
        })

        (k, z, x, y, Raster.tupToRaster(tile, tileExtent))
    }
  }

  def write(tiles: Dataset[KeyedTile], layerName: String, outputURI: URI): Unit = {
    tiles.foreachPartition { rows =>
      val tiles = rows.map {
        case (k, zoom, x, y, raster) =>
          val sk = SpatialKey(x, y)
          val rasterExtent = RasterExtent(raster.extent, raster.tile.cols, raster.tile.rows)

          val features = ArrayBuffer[PointFeature[Int]]()

          raster.tile.foreach { (c: Int, r: Int, value: Int) =>
            if (value > 0) {
              features.append(PointFeature(Point(rasterExtent.gridToMap(c, r)), value))
            }
          }

          (k, zoom, sk, raster.extent, features)
      } map {
        case (k, zoom, sk, tileExtent, features) =>
          val vtFeatures =
            features.map(f => f.mapData(density => Map("density" -> VInt64(density))))

          (k,
           zoom,
           sk,
           tileExtent,
           StrictLayer(
             // TODO use key as the layer name
             name = layerName,
             tileWidth = 4096,
             version = 2,
             tileExtent = tileExtent,
             points = vtFeatures,
             multiPoints = Seq.empty[Feature[MultiPoint, Map[String, Value]]],
             lines = Seq.empty[Feature[Line, Map[String, Value]]],
             multiLines = Seq.empty[Feature[MultiLine, Map[String, Value]]],
             polygons = Seq.empty[Feature[Polygon, Map[String, Value]]],
             multiPolygons = Seq.empty[Feature[MultiPolygon, Map[String, Value]]]
           ))
      } map {
        case (k, zoom, sk, tileExtent, layer) =>
          // TODO use key as the layer name
          val vt = VectorTile(Map(layerName -> layer), tileExtent)

          val byteStream = new ByteArrayOutputStream()

          try {
            val gzipStream = new GZIPOutputStream(byteStream)
            try {
              gzipStream.write(vt.toBytes)
            } finally {
              gzipStream.close()
            }
          } finally {
            byteStream.close()
          }

          (k, zoom, sk.col, sk.row, byteStream.toByteArray)
      }

      // TODO allow size of pool to be increased as "concurrent-uploads-per-executor" or something
      val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(8))

      val parTiles = tiles.toTraversable.par
      // increase the number of concurrent uploads
      parTiles.tasksupport = taskSupport

      parTiles.foreach {
        case (k, z, x, y, bytes) =>
          outputURI.getScheme match {
            case "s3" =>
              val s3Client = S3Client.DEFAULT
              val bucket = outputURI.getHost
              val prefix = outputURI.getPath.drop(1)

              val metadata = new ObjectMetadata()
              metadata.setContentEncoding("gzip")
              metadata.setContentLength(bytes.length)
              try {
                s3Client.putObject(
                  new PutObjectRequest(bucket,
                                       s"$prefix/$k/$z/$x/$y.mvt",
                                       new ByteArrayInputStream(bytes),
                                       metadata))
              } catch {
                case e: AmazonS3Exception => logError(s"Failing writing $k/$z/$x/$y", e)
              }
            case _ =>
              throw new NotImplementedError(s"${outputURI.getScheme} output is not implemented.")
          }
      }

      taskSupport.environment.shutdown()
    }
  }

  def run(historyURI: URI,
          changesetsURI: URI,
          hashtagsURI: Option[URI],
          outputURI: URI,
          footprintType: String): Unit = {
    implicit val spark: SparkSession = Analytics.sparkSession("Footprint")
    import spark.implicits._

    val targetHashtags = hashtagsURI match {
      case Some(uri) =>
        val lines: Seq[String] = uri.getScheme match {
          case "s3" =>
            S3Utils.readText(uri.toString).split("\n")
          case "file" =>
            Files.readAllLines(Paths.get(uri))
          case _ => throw new NotImplementedError(s"${uri.getScheme} scheme is not implemented.")
        }

        lines.filter(_.nonEmpty).map(_.trim).map(_.toLowerCase).toSet
      case None => Set.empty[String]
    }

    val history = footprintType match {
      case "users" =>
        if (targetHashtags.isEmpty) {
          throw new RuntimeException("Refusing to generate footprints for all users")
//          spark.read
//            .orc(historyURI.toString)
//            // use the username as the footprint key
//            .withColumnRenamed("user", "key")
        } else {
          logInfo(s"Finding users who've participated in ${targetHashtags.mkString(", ")}")

          // for hashtag access
          val changesets =
            spark.read
              .orc(changesetsURI.toString)

          val targetUsers = changesets
            .withColumn("hashtag", explode(hashtags('tags)))
            .where('hashtag isin (targetHashtags.toSeq: _*))
            .select('uid)
            .distinct

          spark.read
            .orc(historyURI.toString)
            .join(targetUsers, Seq("uid"))
            // use the username as the footprint key
            .withColumnRenamed("user", "key")
        }
      case "hashtags" =>
        if (targetHashtags.isEmpty) {
//          throw new RuntimeException("Refusing to generate footprints for all hashtags")
          logInfo(s"Finding changesets containing hashtags")
          val changesets =
            spark.read
              .orc(changesetsURI.toString)
              .where(size(hashtags('tags)) > 0)
              .withColumn("hashtag", explode(hashtags('tags)))
              .withColumnRenamed("id", "changeset")

          spark.read
            .orc(historyURI.toString)
            .join(changesets, Seq("changeset"))
            // use the hashtag as the footprint key
            .withColumnRenamed("hashtag", "key")
        } else {
          logInfo(s"Finding changesets containing these hashtags: ${targetHashtags.mkString(", ")}")
          val changesets =
            spark.read
              .orc(changesetsURI.toString)
              .withColumnRenamed("id", "changeset")

          val targetChangesets = changesets
            .withColumn("hashtag", explode(hashtags('tags)))
            .where('hashtag isin (targetHashtags.toSeq: _*))
            .select('changeset, 'hashtag)
            .distinct

          spark.read
            .orc(historyURI.toString)
            .join(targetChangesets, Seq("changeset"))
            // use the hashtag as the footprint key
            .withColumnRenamed("hashtag", "key")
        }
      case _ => throw new RuntimeException("Unrecognized footprint type")
    }

    val layerName = footprintType match {
      case "users"    => "user_footprint"
      case "hashtags" => "hashtag_footprint"
    }

    var tiles = tile(history, BASE_ZOOM).cache

    logInfo(s"Writing ${tiles.count} tiles to zoom ${BASE_ZOOM}...")
    write(tiles, layerName, outputURI)

    for (zoom <- BASE_ZOOM - 1 to 0 by -1) {
      tiles = downsample(tiles).cache
      logInfo(s"Writing ${tiles.count} tiles to zoom ${zoom}...")
      write(tiles, layerName, outputURI)
    }

    spark.stop()
  }
}
