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
import org.apache.spark.rdd.RDD

object GenerateVT {

  type VTF[G <: Geometry] = Feature[G, Map[String, Value]]

  def save(vectorTiles: RDD[(SpatialKey, VectorTile)], zoom: Int, bucket: String, prefix: String) = {
    vectorTiles
      .mapValues(_.toBytes)
      .saveToS3({ sk: SpatialKey => s"s3://${bucket}/${prefix}/${zoom}/${sk.col}/${sk.row}.zip" },
                putObjectModifier = { o => o.withCannedAcl(PublicRead) })
  }

  def apply[G <: Geometry](features: RDD[VTF[G]], layout: LayoutDefinition, layerName: String): RDD[(SpatialKey, VectorTile)] = {
    val keyedGeoms = features.flatMap{ feat =>
      val g = feat.geom
      val keys = layout.mapTransform.keysForGeometry(g)
      keys.map{ k => (k, feat) }
    }

    type FeatureTup = (Seq[VTF[Point]], Seq[VTF[Line]], Seq[VTF[MultiLine]], Seq[VTF[Polygon]], Seq[VTF[MultiPolygon]])

    def create(feat: VTF[Geometry]): FeatureTup = feat.geom match {
      case pt: Point => (Seq(PointFeature(pt, feat.data)), Seq.empty, Seq.empty, Seq.empty, Seq.empty)
      case l: Line => (Seq.empty, Seq(LineFeature(l, feat.data)), Seq.empty, Seq.empty, Seq.empty)
      case ml: MultiLine => (Seq.empty, Seq.empty, Seq(MultiLineFeature(ml, feat.data)), Seq.empty, Seq.empty)
      case p: Polygon => (Seq.empty, Seq.empty, Seq.empty, Seq(PolygonFeature(p, feat.data)), Seq.empty)
      case mp: MultiPolygon => (Seq.empty, Seq.empty, Seq.empty, Seq.empty, Seq(MultiPolygonFeature(mp, feat.data)))
    }

    def merge(tup: FeatureTup, feat: VTF[Geometry]) =
      combine(tup, create(feat))

    def combine(tup1: FeatureTup, tup2: FeatureTup) = {
      val (pt1, l1, ml1, p1, mp1) = tup1
      val (pt2, l2, ml2, p2, mp2) = tup2
      (pt1 ++ pt2, l1 ++ l2, ml1 ++ ml2, p1 ++ p2, mp1 ++ mp2)
    }

    keyedGeoms
      .combineByKey(create, merge, combine)
      .map { case (sk, tup) => {
        val (pts, ls, mls, ps, mps) = tup
        val extent = layout.mapTransform(sk)

        val layer = StrictLayer(
          name=layerName,
          tileWidth=4096,
          version=2,
          tileExtent=extent,
          points=pts,
          multiPoints=Seq.empty,
          lines=ls,
          multiLines=mls,
          polygons=ps,
          multiPolygons=mps
        )

        (sk, VectorTile(Map(layerName -> layer), extent))
      }}
  }

}
