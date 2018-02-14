package osmesa

import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.raster.rasterize.polygon._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.vectortile._
import org.apache.spark.rdd.RDD

object GenerateVT {

  type VTF[D <: Geometry] = Feature[D, ElementMeta]

  def apply(features: RDD[VTF[Geometry]], layout: LayoutDefinition) = {
    val re = RasterExtent(layout.extent, layout.layoutCols, layout.layoutRows)
    val keyedGeoms = features.flatMap{ feat =>
      val g = feat.geom
      val keys = geometryToGridCells(g, re)
      keys.map{ k => (k, feat) }
    }

    type FeatureTup = (Seq[VTF[Point]], Seq[VTF[Line]], Seq[VTF[MultiLine]], Seq[VTF[Polygon]], Seq[VTF[MultiPolygon]])

    def create(feat: VTF[Geometry]): FeatureTup = feat.geom match {
      case pt: Point => (Seq(PointFeature(pt, feat.data)), Seq.empty, Seq.empty, Seq.empty, Seq.empty)
      case l: Line => (Seq.empty, Seq(LineFeature(l, feat.data)), Seq.empty, Seq.empty, Seq.empty)
      case ml: MultiLine => (Seq.empty, Seq.empty, Seq(MultiLineFeature(ml, feat.data)), Seq.empty, Seq.empty)
      case p: Polygon => (Seq.empty, Seq.empty, Seq.empty, Seq(PolygonFeature(p, feat.data)), Seq.empty)
      case mp: MultiPoly => (Seq.empty, Seq.empty, Seq.empty, Seq.empty, Seq(MultiPolygonFeature(mp, feat.data)))
    }

    def merge(tup: FeatureTup, feat: VTF[Geometry]) =
      combine(tup, create(feat))

    def combine(tup1: FeatureTup, tup2: FeatureTup) = {
      val (pt1, l1, ml1, p1, mp1) = tup1
      val (pt2, l2, ml2, p2, mp2) = tup2
      (pt1 ++ pt2, l1 ++ l2, ml1 ++ ml2, p1 ++ p2, mp1 ++ mp2)
    }

    def vtFeatureToElement[G <: Geometry](feat: VTF[G]): Feature[G, Map[String, Value]] = {

    }

    def tupToLayer(tup: FeatureTup): Layer = {
      val (pts, ls, mls, ps, mps) = tup

    }

    keyedGeoms
      .combineByKey(create, merge, combine)
      .map(
  }

  private def geometryToGridCells(g: Geometry, re: RasterExtent): Seq[SpatialKey] = {
    g match {
      case pt: Point => pointToGridCells(pt, re)
      case l: Line => lineToGridCells(l, re)
      case ml: MultiLine => multiLineToGridCells(ml, re)
      case p: Polygon => polygonToGridCells(p, re)
      case mp: MultiPolygon => multiPolygonToGridCells(mp, re)
    }
  }

  private def pointToGridCells(point: Point, re: RaterExtent): Seq[SpatialKey] = {
    val (x, y) = re.mapToGrid(point)
    Seq(SpatialKey(x, y))
  }

  private def polygonToGridCells(poly: Polygon, re: RasterExtent): Seq[SpatialKey] = {
    val keys = collection.mutable.Set.empty[SpatialKey]
    FractionalRasterizer.foreachCellByPolygon(poly, re)( new FractionCallback {
      def callback(col: Int, row: Int, fraction:Double) = keys += SpatialKey(col, row)
    })
    keys.toSeq
  }

  private def multiPolygonToGridCells(mp: MultiPolygon, re: RasterExtent): Seq[SpatialKey] = {
    val keys = collection.mutable.Set.empty[SpatialKey]
    FractionalRasterizer.foreachCellByMultiPolygon(mp, re)( new FractionCallback {
      def callback(col: Int, row: Int, fraction:Double) = keys += SpatialKey(col, row)
    })
    keys.toSeq
  }

  private def lineToGridCells(line: Line, re: RasterExtent): Seq[SpatialKey] = {
    val keys = collection.mutable.Set.empty[SpatialKey]
    Rasterizer.foreachCellByLineStringDouble(line, re){ (x, y) => keys += SpatialKey(x, y) }
    keys.toSeq
  }

  private def multiLineToGridCells(ml: MultiLine, re: RasterExtent): Seq[SpatialKey] = {
    val keys = collection.mutable.Set.empty[SpatialKey]
    ml.lines.foreach { line =>
      Rasterizer.foreachCellByLineStringDouble(line, re){ (x, y) => keys += SpatialKey(x, y) }
    }
    keys.toSeq
  }

}
