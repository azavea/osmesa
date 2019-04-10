package osmesa.analytics

import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point}
import geotrellis.raster.{Raster, Tile}
import geotrellis.spark.SpatialKey
import geotrellis.vector.{Extent, GeomFactory, PointFeature}
import osmesa.common.raster._

package object vectorgrid {
  case class CoordinatesWithKey(key: String, lat: Option[Double], lon: Option[Double]) {
    def geom: Point = GeomFactory.factory.createPoint(new Coordinate(x, y))

    def x: Double = lon.map(_.doubleValue).getOrElse(Double.NaN)
    def y: Double = lat.map(_.doubleValue).getOrElse(Double.NaN)
  }

  case class CoordinatesWithKeyAndSequence(sequence: Int,
                                           key: String,
                                           lat: Option[Double],
                                           lon: Option[Double]) {
    def geom: Point = GeomFactory.factory.createPoint(new Coordinate(x, y))

    def x: Double = lon.map(_.doubleValue).getOrElse(Double.NaN)
    def y: Double = lat.map(_.doubleValue).getOrElse(Double.NaN)
  }

  case class GeometryTileWithKey(key: String, zoom: Int, sk: SpatialKey, geom: Geometry)

  case class GeometryTileWithKeyAndSequence(sequence: Int,
                                            key: String,
                                            zoom: Int,
                                            sk: SpatialKey,
                                            geom: Geometry)

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
    lazy val raster: Raster[Tile] = (
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
    lazy val raster: Raster[Tile] = (
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
    lazy val raster: Raster[Tile] = (
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
