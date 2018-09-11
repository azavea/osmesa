package osmesa.analytics

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.zip.GZIPOutputStream

import com.amazonaws.services.s3.model.{AmazonS3Exception, ObjectMetadata}
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.spark.SpatialKey
import geotrellis.spark.io.s3.{AmazonS3Client, S3Client}
import geotrellis.spark.tiling.{LayoutDefinition, ZoomedLayoutScheme}
import geotrellis.vector.io._
import geotrellis.vector.io.json.JsonFeatureCollectionMap
import geotrellis.vector.{Extent, Feature, Geometry, Line, MultiLine, MultiPoint, MultiPolygon, Point, Polygon}
import geotrellis.vectortile._
import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging
import osmesa.analytics.updater.Implicits._
import osmesa.common.model.ElementWithSequence

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.{Failure, Success, Try}

package object updater extends Logging {
  private lazy val s3: AmazonS3Client = S3Client.DEFAULT

  type AugmentedDiffFeature = Feature[Geometry, ElementWithSequence]
  type VTFeature = Feature[Geometry, VTProperties]
  type TypedVTFeature[T <: Geometry] = Feature[T, VTProperties]
  type VTProperties = Map[String, Value]

  val LayoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator)

  private def createMetadata(contentLength: Int, encoding: Option[String]): ObjectMetadata = {
    val meta = new ObjectMetadata()

    meta.setContentLength(contentLength)
    encoding.foreach(meta.setContentEncoding)

    meta
  }

  def read(uri: URI): Option[Array[Byte]] = {
    uri.getScheme match {
      case "s3" =>
        Try(
          IOUtils.toByteArray(
            s3.getObject(uri.getHost,
                         URLDecoder.decode(uri.getPath.drop(1), StandardCharsets.UTF_8.toString))
              .getObjectContent)) match {
          case Success(bytes) => Some(bytes)
          case Failure(e) =>
            e match {
              case ex: AmazonS3Exception if ex.getErrorCode == "NoSuchKey" =>
              case ex: AmazonS3Exception =>
                logWarning(s"Could not read $uri: ${ex.getMessage}")
              case _ =>
                logWarning(s"Could not read $uri: $e")
            }

            None
        }
      case "file" =>
        val path = Paths.get(uri)

        if (Files.exists(path)) {
          Some(Files.readAllBytes(path))
        } else {
          None
        }
    }
  }

  def readFeatures(uri: URI): Option[Seq[(Option[AugmentedDiffFeature], AugmentedDiffFeature)]] = {
    val lines: Option[Seq[String]] = uri.getScheme match {
      case "s3" =>
        Try(
          IOUtils.toString(
            s3.getObject(uri.getHost,
                         URLDecoder.decode(uri.getPath.drop(1), StandardCharsets.UTF_8.toString)).getObjectContent)) match {
          case Success(content) => Some(content.split("\n"))
          case Failure(e) =>
            e match {
              case ex: AmazonS3Exception if ex.getErrorCode == "NoSuchKey" =>
              case ex: AmazonS3Exception =>
                logWarning(s"Could not read $uri: ${ex.getMessage}")
              case _ =>
                logWarning(s"Could not read $uri: $e")
            }

            None
        }
      case "file" =>
        val path = Paths.get(uri)

        if (Files.exists(path)) {
          Some(Source.fromFile(uri).getLines.toSeq)
        } else {
          None
        }
    }

    lines match {
      case Some(ls) =>
        val features = ls
          .map(
            _.replace("\u001e", "") // remove record separators if present
              .parseGeoJson[JsonFeatureCollectionMap]
              .getAll[AugmentedDiffFeature])

        Some(features.map(_.get("old")).zip(features.map(_("new"))))
      case None => None
    }
  }

  def write(uri: URI, bytes: Array[Byte], encoding: Option[String] = None): Any =
    uri.getScheme match {
      case "s3" =>
        Try(
          s3.putObject(uri.getHost,
                       URLDecoder.decode(uri.getPath.drop(1), StandardCharsets.UTF_8.toString),
                       new ByteArrayInputStream(bytes),
                       createMetadata(bytes.length, encoding))) match {
          case Success(_) =>
          case Failure(e) =>
            e match {
              case ex: AmazonS3Exception =>
                logWarning(s"Could not write $uri: ${ex.getMessage}")
              case _ =>
                logWarning(s"Could not write $uri: $e")
            }
        }
      case "file" =>
        Files.write(Paths.get(uri), bytes)
    }

  /**
    * Write a vector tile to a URI.
    *
    * @param vectorTile Vector tile to serialize.
    * @param uri URI to write to.
    * @return
    */
  def write(vectorTile: VectorTile, uri: URI): Any = {
    val byteStream = new ByteArrayOutputStream()

    try {
      val gzipStream = new GZIPOutputStream(byteStream)
      try {
        gzipStream.write(vectorTile.toBytes)
      } finally {
        gzipStream.close()
      }
    } finally {
      byteStream.close()
    }

    write(uri, byteStream.toByteArray, Some("gzip"))
  }

  def tile(features: Seq[(Option[AugmentedDiffFeature], AugmentedDiffFeature)],
           layout: LayoutDefinition)
    : Map[SpatialKey, Seq[(Option[AugmentedDiffFeature], AugmentedDiffFeature)]] =
    features
      .map {
        case (prev, curr) =>
          (prev.map(_.mapGeom(_.reproject(LatLng, WebMercator))),
           curr.mapGeom(_.reproject(LatLng, WebMercator)))
      }
      .filter {
        case (_, curr) => curr.isValid
      }
      .flatMap {
        case (prev, curr) =>
          val prevKeys = prev
            .map(f => layout.mapTransform.keysForGeometry(f.geom))
            .getOrElse(Set.empty[SpatialKey])
          val currKeys = layout.mapTransform.keysForGeometry(curr.geom)

          (prevKeys ++ currKeys)
            .map { sk =>
              (sk,
               (prev.map(_.mapGeom(_.intersection(sk.extent(layout)).toGeometry.orNull)),
                curr.mapGeom(_.intersection(sk.extent(layout)).toGeometry.orNull)))
            }
      }
      .groupBy(_._1)
      .mapValues(_.map(_._2))

  def path(zoom: Int, sk: SpatialKey) = s"$zoom/${sk.col}/${sk.row}.mvt"

  def updateTiles(tileSource: URI,
                  zoom: Int,
                  schemaType: SchemaBuilder,
                  features: Seq[(Option[AugmentedDiffFeature], AugmentedDiffFeature)],
                  listing: Option[Path],
                  process: (SpatialKey, VectorTile) => Any): Unit = {
    val layout = LayoutScheme.levelForZoom(zoom).layout

    // NOTE: data will exist for both generations of features in a given SpatialKey; geometries may be null if a
    // generation's geometry did not intersect that key
    val tiledFeatures = tile(features, layout)

    // NOTE: when writing features to a tile that was not previously intersected, minorVersion (and authors) will be
    // wrong

    val tiles = listing match {
      case Some(l) =>
        Source.fromFile(l.toFile).getLines.toSet
      case None => Set.empty[String]
    }

    tiledFeatures
      .filter {
        case (sk, _) => tiles.isEmpty || tiles.contains(path(zoom, sk))
      }
      .par
      .foreach {
        case (sk, feats) =>
          val filename = path(zoom, sk)
          val uri = tileSource.resolve(filename)

          read(uri) match {
            case Some(bytes) =>
              val extent = sk.extent(layout)
              val tile = VectorTile.fromBytes(bytes, extent)

              val featuresById: Map[String, (Option[AugmentedDiffFeature], AugmentedDiffFeature)] =
                feats
                  .groupBy(_._2.data.elementId)
                  .mapValues(fs => fs.head)
              val featureIds = featuresById.keySet

              // load the target layer
              val layer = tile.layers(schemaType.layerName)

              logDebug(
                s"Inspecting ${layer.features.size.formatted("%,d")} features in layer '${schemaType.layerName}'")

              // fetch unmodified features
              val unmodifiedFeatures = layer.features
                .filterNot(f => featureIds.contains(f.data("__id")))

              val schema: Schema = schemaType(layer, featuresById)

              val retainedFeatures = schema.retainedFeatures
              val replacementFeatures = schema.replacementFeatures
              val newFeatures = schema.newFeatures

              if (newFeatures.nonEmpty) {
                logInfo(s"Writing ${unmodifiedFeatures.length
                  .formatted("%,d")} + ${newFeatures.length.formatted("%,d")} feature(s)")
              }

              unmodifiedFeatures ++ retainedFeatures ++ replacementFeatures ++ newFeatures match {
                case updatedFeatures if (replacementFeatures.length + newFeatures.length) > 0 =>
                  val updatedLayer = makeLayer(schemaType.layerName, extent, updatedFeatures)

                  // merge all available layers into a new tile
                  val newTile =
                    VectorTile(tile.layers.updated(updatedLayer._1, updatedLayer._2), extent)

                  process(sk, newTile)
                case _ =>
                  logInfo(s"No changes to $uri; skipping")
              }
            case None =>
          }
      }
  }

  def segregate(features: Seq[VTFeature]): (Seq[TypedVTFeature[Point]],
                                            Seq[TypedVTFeature[MultiPoint]],
                                            Seq[TypedVTFeature[Line]],
                                            Seq[TypedVTFeature[MultiLine]],
                                            Seq[TypedVTFeature[Polygon]],
                                            Seq[TypedVTFeature[MultiPolygon]]) = {
    val points = ListBuffer[TypedVTFeature[Point]]()
    val multiPoints = ListBuffer[TypedVTFeature[MultiPoint]]()
    val lines = ListBuffer[TypedVTFeature[Line]]()
    val multiLines = ListBuffer[TypedVTFeature[MultiLine]]()
    val polygons = ListBuffer[TypedVTFeature[Polygon]]()
    val multiPolygons = ListBuffer[TypedVTFeature[MultiPolygon]]()

    features.foreach {
      case f @ Feature(g: Point, _: Any) => points += f.mapGeom[Point](_.as[Point].get)
      case f @ Feature(g: MultiPoint, _: Any) =>
        multiPoints += f.mapGeom[MultiPoint](_.as[MultiPoint].get)
      case f @ Feature(g: Line, _: Any) => lines += f.mapGeom[Line](_.as[Line].get)
      case f @ Feature(g: MultiLine, _: Any) =>
        multiLines += f.mapGeom[MultiLine](_.as[MultiLine].get)
      case f @ Feature(g: Polygon, _: Any) => polygons += f.mapGeom[Polygon](_.as[Polygon].get)
      case f @ Feature(g: MultiPolygon, _: Any) =>
        multiPolygons += f.mapGeom[MultiPolygon](_.as[MultiPolygon].get)
    }

    (points, multiPoints, lines, multiLines, polygons, multiPolygons)
  }

  def makeLayer(name: String, extent: Extent, features: Seq[VTFeature]): (String, Layer) = {
    val (points, multiPoints, lines, multiLines, polygons, multiPolygons) = segregate(features)

    name -> StrictLayer(
      name = name,
      tileWidth = 4096,
      version = 2,
      tileExtent = extent,
      points = points,
      multiPoints = multiPoints,
      lines = lines,
      multiLines = multiLines,
      polygons = polygons,
      multiPolygons = multiPolygons
    )
  }
}
