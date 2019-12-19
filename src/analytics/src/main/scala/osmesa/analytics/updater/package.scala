package osmesa.analytics

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException}
import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.zip.GZIPOutputStream

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.layer.SpatialKey
import geotrellis.layer.{LayoutDefinition, ZoomedLayoutScheme}
import geotrellis.store.s3.S3ClientProducer
import geotrellis.vector._
import geotrellis.vector.io.json.JsonFeatureCollectionMap
import geotrellis.vectortile._
import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging
import osmesa.analytics.updater.Implicits._
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  GetObjectRequest, NoSuchKeyException, PutObjectRequest, S3Exception
}
import vectorpipe.model.ElementWithSequence

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.{Failure, Success, Try}

package object updater extends Logging {
  private lazy val s3: S3Client = S3ClientProducer.get()

  type AugmentedDiffFeature = Feature[Geometry, ElementWithSequence]
  type VTFeature = Feature[Geometry, VTProperties]
  type TypedVTFeature[T <: Geometry] = Feature[T, VTProperties]
  type VTProperties = Map[String, Value]

  val LayoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator)

  def read(uri: URI): Option[Array[Byte]] = {
    uri.getScheme match {
      case "s3" => {
        val key = URLDecoder.decode(uri.getPath.drop(1), StandardCharsets.UTF_8.toString)
        val request = GetObjectRequest.builder()
          .bucket(uri.getHost)
          .key(key)
          .build()
        Try(IOUtils.toByteArray(s3.getObjectAsBytes(request).asInputStream)) match {
          case Success(bytes) => Some(bytes)
          case Failure(e) =>
            e match {
              case _: NoSuchKeyException => None
              // treat these exceptions as fatal, as treating tiles as None has
              // the potential to corrupt the tileset
              case _ =>
                logWarning(s"Could not read $uri: ${e.getMessage}", e)
                throw e
            }
        }
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
      case "s3" => {
        val utf8 = StandardCharsets.UTF_8
        val key = URLDecoder.decode(uri.getPath.drop(1), utf8.toString)
        val request = GetObjectRequest.builder()
          .bucket(uri.getHost)
          .key(key)
          .build()
        Try(IOUtils.toString(s3.getObjectAsBytes(request).asInputStream, utf8)) match {
          case Success(content) => Some(content.split("\n"))
          case Failure(e) =>
            e match {
              case _: NoSuchKeyException =>
              case ex: S3Exception =>
                logWarning(s"Could not read $uri: ${ex.getMessage}")
              case _ =>
                logWarning(s"Could not read $uri: $e")
            }
            None
        }
      }
      case "file" =>
        val path = Paths.get(uri)

        if (Files.exists(path)) {
          var source: Option[Source] = None

          try {
            source = Some(Source.fromFile(uri))
            source.map(_.getLines.toSeq)
          } catch {
            case e: IOException =>
              logWarning(s"Failed to read ${path}", e)

              None
          } finally {
            try {
              source.foreach(_.close)
            } catch {
              case e: IOException => logWarning(s"Failed to close ${path}", e)
            }
          }
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
      case "s3" => {
        val utf8 = StandardCharsets.UTF_8
        val key = URLDecoder.decode(uri.getPath.drop(1), utf8.toString)
        val builder = PutObjectRequest.builder()
          .bucket(uri.getHost)
          .key(key)
          .contentLength(bytes.length.toLong)
        if (encoding.isDefined) {
          builder.contentEncoding(encoding.get)
        }
        Try(s3.putObject(builder.build, RequestBody.fromBytes(bytes))) match {
          case Success(_) =>
          case Failure(e) =>
            e match {
              case ex: S3Exception =>
                logWarning(s"Could not write $uri: ${ex.getMessage}")
              case _ =>
                logWarning(s"Could not write $uri: $e")
            }
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
               // TODO: Error check intersection computations
               (prev.map(_.mapGeom(_.intersection(sk.extent(layout)))),
                curr.mapGeom(_.intersection(sk.extent(layout)))))
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

  def segregate(features: Iterable[MVTFeature[Geometry]]): (Seq[MVTFeature[Point]],
                                                 Seq[MVTFeature[MultiPoint]],
                                                 Seq[MVTFeature[LineString]],
                                                 Seq[MVTFeature[MultiLineString]],
                                                 Seq[MVTFeature[Polygon]],
                                                 Seq[MVTFeature[MultiPolygon]]) = {
    val points = ListBuffer[MVTFeature[Point]]()
    val multiPoints = ListBuffer[MVTFeature[MultiPoint]]()
    val lines = ListBuffer[MVTFeature[LineString]]()
    val multiLines = ListBuffer[MVTFeature[MultiLineString]]()
    val polygons = ListBuffer[MVTFeature[Polygon]]()
    val multiPolygons = ListBuffer[MVTFeature[MultiPolygon]]()

    features.foreach {
      case f @ MVTFeature(i, g: Point, d: Map[String, Value]) =>
        points += MVTFeature(i, g.asInstanceOf[Point], d)
      case f @ MVTFeature(i, g: MultiPoint, d: Map[String, Value]) =>
        multiPoints += MVTFeature(i, g.asInstanceOf[MultiPoint], d)
      case f @ MVTFeature(i, g: LineString, d: Map[String, Value]) =>
        lines += MVTFeature(i, g.asInstanceOf[LineString], d)
      case f @ MVTFeature(i, g: MultiLineString, d: Map[String, Value]) =>
        multiLines += MVTFeature(i, g.asInstanceOf[MultiLineString], d)
      case f @ MVTFeature(i, g: Polygon, d: Map[String, Value]) =>
        polygons += MVTFeature(i, g.asInstanceOf[Polygon], d)
      case f @ MVTFeature(i, g: MultiPolygon, d: Map[String, Value]) =>
        multiPolygons += MVTFeature(i, g.asInstanceOf[MultiPolygon], d)
    }

    (points, multiPoints, lines, multiLines, polygons, multiPolygons)
  }

  def makeLayer(name: String, extent: Extent, features: Iterable[MVTFeature[Geometry]], tileWidth: Int = 4096): (String, Layer) = {
    val (points, multiPoints, lines, multiLines, polygons, multiPolygons) = segregate(features)

    name -> StrictLayer(
      name = name,
      tileWidth = tileWidth,
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
