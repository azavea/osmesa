package osmesa.analytics

import org.locationtech.jts.geom.{Coordinate, Geometry, Point}
import geotrellis.raster.{Raster, Tile}
import geotrellis.spark.SpatialKey
import geotrellis.vector.{Extent, GeomFactory, PointFeature}
import osmesa.analytics.raster._

package object vectorgrid {
  case class PointWithKey(key: String, geom: Point)

  case class PointWithKeyAndFacets(key: String, geom: Point, facets: Map[String, Int])

  case class PointWithKeyAndSequence(sequence: Int, key: String, geom: Point)

  case class PointWithKeyAndFacetsAndSequence(sequence: Int,
                                              key: String,
                                              geom: Point,
                                              facets: Map[String, Int])

  case class GeometryTileWithKey(key: String,
                                 zoom: Int,
                                 sk: SpatialKey,
                                 geom: Geometry,
                                 value: Int = 1)

  case class GeometryTileWithKeyAndSequence(sequence: Int,
                                            key: String,
                                            zoom: Int,
                                            sk: SpatialKey,
                                            geom: Geometry,
                                            value: Int = 1)

  case class RasterTileWithKeyAndSequence(sequence: Int,
                                          key: String,
                                          zoom: Int,
                                          sk: SpatialKey,
                                          values: Map[Long, Int],
                                          cols: Int,
                                          rows: Int,
                                          xmin: Double,
                                          ymin: Double,
                                          xmax: Double,
                                          ymax: Double) {
    lazy val raster = Raster[Tile](
      SparseIntTile(cols, rows, values),
      Extent(xmin, ymin, xmax, ymax)
    )
  }

  case class RasterWithSequence(values: Map[Long, Int],
                                cols: Int,
                                rows: Int,
                                xmin: Double,
                                ymin: Double,
                                xmax: Double,
                                ymax: Double,
                                sequence: Int) {
    lazy val raster = Raster[Tile](
      SparseIntTile(cols, rows, values),
      Extent(xmin, ymin, xmax, ymax)
    )
  }

  case class RasterWithSequenceTileSeqWithTileCoordinatesAndKey(tiles: Seq[RasterWithSequence],
                                                                zoom: Int,
                                                                sk: SpatialKey,
                                                                key: String)

  case class RasterTileWithKey(key: String,
                               zoom: Int,
                               sk: SpatialKey,
                               values: Map[Long, Int],
                               cols: Int,
                               rows: Int,
                               xmin: Double,
                               ymin: Double,
                               xmax: Double,
                               ymax: Double) {
    lazy val raster = Raster[Tile](
      SparseIntTile(cols, rows, values),
      Extent(xmin, ymin, xmax, ymax)
    )
  }

  case class VectorTileWithKey(key: String,
                               zoom: Int,
                               sk: SpatialKey,
                               features: Seq[PointFeature[Map[String, Long]]])

  case class VectorTileWithKeyAndSequence(sequence: Int,
                                          key: String,
                                          zoom: Int,
                                          sk: SpatialKey,
                                          features: Seq[PointFeature[Map[String, Long]]])

  case class VectorTileWithSequence(sequence: Int,
                                    zoom: Int,
                                    sk: SpatialKey,
                                    features: Seq[PointFeature[Map[String, Long]]])

  case class VectorTileWithSequences(zoom: Int,
                                     sk: SpatialKey,
                                     features: Seq[PointFeature[Map[String, Long]]],
                                     sequences: Set[Int])

  object RasterTileWithKey {
    def apply(key: String, zoom: Int, sk: SpatialKey, raster: Raster[Tile]): RasterTileWithKey =
      RasterTileWithKey(
        key,
        zoom,
        sk,
        raster.toMap,
        raster.cols,
        raster.rows,
        raster.extent.xmin,
        raster.extent.ymin,
        raster.extent.xmax,
        raster.extent.ymax
      )
  }

  object RasterWithSequence {
    def apply(raster: Raster[Tile], sequence: Int): RasterWithSequence =
      RasterWithSequence(
        raster.toMap,
        raster.cols,
        raster.rows,
        raster.extent.xmin,
        raster.extent.ymin,
        raster.extent.xmax,
        raster.extent.ymax,
        sequence
      )
  }

  object RasterTileWithKeyAndSequence {
    def apply(sequence: Int,
              key: String,
              zoom: Int,
              sk: SpatialKey,
              raster: Raster[Tile]): RasterTileWithKeyAndSequence =
      RasterTileWithKeyAndSequence(
        sequence,
        key,
        zoom,
        sk,
        raster.toMap,
        raster.cols,
        raster.rows,
        raster.extent.xmin,
        raster.extent.ymin,
        raster.extent.xmax,
        raster.extent.ymax
      )
  }
}
