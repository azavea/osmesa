package osmesa

import com.amazonaws.services.s3.model.CannedAccessControlList._
import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.raster.rasterize.polygon._
import geotrellis.spark._
import geotrellis.spark.io.s3._
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.vectortile._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object GenerateVT {

  lazy val logger = Logger.getRootLogger()

  type VTF[G <: Geometry] = Feature[G, Map[String, Value]]
  // type VTContents = (Seq[VTF[Point]], Seq[VTF[MultiPoint]], Seq[VTF[Line]], Seq[VTF[MultiLine]], Seq[VTF[Polygon]], Seq[VTF[MultiPolygon]])

  case class VTContents(points: Array[VTF[Point]] = Array.empty,
                                multipoints: Array[VTF[MultiPoint]] = Array.empty,
                                lines: Array[VTF[Line]] = Array.empty,
                                multilines: Array[VTF[MultiLine]] = Array.empty,
                                polygons: Array[VTF[Polygon]] = Array.empty,
                                multipolygons: Array[VTF[MultiPolygon]] = Array.empty) {
    def +(other: VTContents) = VTContents(points ++ other.points,
                                          multipoints ++ other.multipoints,
                                          lines ++ other.lines,
                                          multilines ++ other.multilines,
                                          polygons ++ other.polygons,
                                          multipolygons ++ other.multipolygons)
  }
  object VTContents {
    def empty() = VTContents(Array.empty, Array.empty, Array.empty, Array.empty, Array.empty, Array.empty)
  }

  def save(vectorTiles: RDD[(SpatialKey, VectorTile)], zoom: Int, bucket: String, prefix: String) = {
    vectorTiles
      .mapValues(_.toBytes)
      .saveToS3({ sk: SpatialKey => s"s3://${bucket}/${prefix}/${zoom}/${sk.col}/${sk.row}.mvt" },
                putObjectModifier = { o => o.withCannedAcl(PublicRead) })
  }

  def keyToLayout[G <: Geometry](features: RDD[VTF[G]], layout: LayoutDefinition): RDD[(SpatialKey, (SpatialKey, VTF[G]))] = {
    features.flatMap{ feat =>
      val g = feat.geom
      val keys = layout.mapTransform.keysForGeometry(g)
      keys.flatMap{ k =>
        val SpatialKey(x, y) = k
        if (x < 0 || x >= layout.layoutCols || y < 0 || y >= layout.layoutRows) {
          println(s"Geometry $g exceeds layout bounds in $k (${Try(layout.mapTransform(k))})")
          None
        } else {
          Some(k -> (k, feat))
        }
      }
    }
  }

  def upLevel[G <: Geometry](keyedGeoms: RDD[(SpatialKey, (SpatialKey, VTF[G]))]): RDD[(SpatialKey, (SpatialKey, VTF[G]))] = {
    keyedGeoms.map{ case (key, (_, feat)) => {
      val SpatialKey(x, y) = key
      val newKey = SpatialKey(x/2, y/2)
      (newKey, (newKey, feat))
    }}
  }

  def makeVectorTiles[G <: Geometry](keyedGeoms: RDD[(SpatialKey, (SpatialKey, VTF[G]))], layout: LayoutDefinition, layerName: String): RDD[(SpatialKey, VectorTile)] = {

    def timedIntersect[G <: Geometry](geom: G, ex: Extent, id: Long) = {
      val future = Future { geom.intersection(ex) }
      Try(Await.result(future, 5000 milliseconds)) match {
        case Success(res) => res
        case Failure(_) =>
          logger.warn(s"Could not intersect $geom with $ex [feature id=$id] in 5000 milliseconds")
          NoResult
      }
    }

    def create(arg: (SpatialKey, VTF[Geometry])): VTContents = {
      val (sk, feat) = arg
      val fid = feat.data("__id").asInstanceOf[VInt64].value
      val baseEx = layout.mapTransform(sk)
      val ex = Extent(baseEx.xmin - baseEx.width, baseEx.ymin - baseEx.height, baseEx.xmax + baseEx.width, baseEx.ymax + baseEx.height)
      feat.geom match {
        case pt: Point => VTContents(points = Array(PointFeature(pt, feat.data)))
        case l: Line =>
          timedIntersect(l, ex, fid) match {
            case LineResult(res) => VTContents(lines=Array(LineFeature(res, feat.data)))
            case MultiLineResult(res) => VTContents(multilines=Array(MultiLineFeature(res, feat.data)))
            case GeometryCollectionResult(res) =>
              Try(res.geometryCollections(0)).toOption match {
                case Some(gc) =>
                  gc.lines.size match {
                    case 0 => VTContents.empty // should never happen
                    case 1 => VTContents(lines=Array(LineFeature(gc.lines(0), feat.data)))
                    case _ => VTContents(multilines=Array(MultiLineFeature(MultiLine(gc.lines), feat.data)))
                  }
                case None =>
                  logger.warn(s"Unexpected result intersecting $l with $ex")
                  VTContents.empty
              }
            case _ => VTContents.empty
          }
        case ml: MultiLine =>
          timedIntersect(ml, ex, fid) match {
            case LineResult(res) => VTContents(lines=Array(LineFeature(res, feat.data)))
            case MultiLineResult(res) => VTContents(multilines=Array(MultiLineFeature(res, feat.data)))
            case GeometryCollectionResult(res) =>
              Try(res.geometryCollections(0)).toOption match {
                case Some(gc) =>
                  gc.lines.size match {
                    case 0 => VTContents.empty // should never happen
                    case 1 => VTContents(lines=Array(LineFeature(gc.lines(0), feat.data)))
                    case _ => VTContents(multilines=Array(MultiLineFeature(MultiLine(gc.lines), feat.data)))
                  }
                case None =>
                  logger.warn(s"Unexpected result intersecting $ml with $ex")
                  VTContents.empty
              }
            case _ => VTContents.empty
          }
        case p: Polygon =>
          timedIntersect(p, ex, fid) match {
            // should only see (or care about) polygon intersection results
            case PolygonResult(res) => VTContents(polygons=Array(PolygonFeature(res, feat.data)))
            case MultiPolygonResult(res) => VTContents(multipolygons=Array(MultiPolygonFeature(res, feat.data)))
            case GeometryCollectionResult(res) =>
              Try(res.geometryCollections(0)).toOption match {
                case Some(gc) =>
                  gc.polygons.size match {
                    case 0 => VTContents.empty // should never happen
                    case 1 => VTContents(polygons=Array(PolygonFeature(gc.polygons(0), feat.data)))
                    case _ => VTContents(multipolygons=Array(MultiPolygonFeature(MultiPolygon(gc.polygons), feat.data)))
                  }
                case None =>
                  logger.warn(s"Unexpected result intersecting $p with $ex")
                  VTContents.empty
              }
            case _ => VTContents.empty
          }
        case mp: MultiPolygon =>
          timedIntersect(mp, ex, fid) match {
            // should only see (or care about) polygon intersection results
            case PolygonResult(res) => VTContents(polygons=Array(PolygonFeature(res, feat.data)))
            case MultiPolygonResult(res) => VTContents(multipolygons=Array(MultiPolygonFeature(res, feat.data)))
            case GeometryCollectionResult(res) =>
              Try(res.geometryCollections(0)).toOption match {
                case Some(gc) =>
                  gc.polygons.size match {
                    case 0 => VTContents.empty // should never happen
                    case 1 => VTContents(polygons=Array(PolygonFeature(gc.polygons(0), feat.data)))
                    case _ => VTContents(multipolygons=Array(MultiPolygonFeature(MultiPolygon(gc.polygons), feat.data)))
                  }
                case None =>
                  logger.warn(s"Unexpected result intersecting $mp with $ex")
                  VTContents.empty
              }
            case _ => VTContents.empty
          }
      }
    }

    def merge(accum: VTContents, feat: (SpatialKey, VTF[Geometry])): VTContents =
      accum + create(feat)

    keyedGeoms
      .combineByKey(create, merge, (_: VTContents) + (_: VTContents))
      .map { case (sk, tup) => {
        val VTContents(pts, mpts, ls, mls, ps, mps) = tup
        val extent = layout.mapTransform(sk)

        val layer = StrictLayer(
          name=layerName,
          tileWidth=4096,
          version=2,
          tileExtent=extent,
          points=pts,
          multiPoints=mpts,
          lines=ls,
          multiLines=mls,
          polygons=ps,
          multiPolygons=mps
        )

        (sk, VectorTile(Map(layerName -> layer), extent))
      }}
  }

}
