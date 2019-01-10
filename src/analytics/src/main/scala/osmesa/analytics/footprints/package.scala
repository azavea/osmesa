package osmesa.analytics

import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point}
import geotrellis.raster.{Raster, Tile}
import geotrellis.vector.{Extent, GeomFactory}
import osmesa.common.raster._

package object footprints {
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

  case class GeometryTileWithKey(key: String, zoom: Int, x: Int, y: Int, geom: Geometry)

  case class GeometryTileWithKeyAndSequence(sequence: Int,
                                            key: String,
                                            zoom: Int,
                                            x: Int,
                                            y: Int,
                                            geom: Geometry)

  case class RasterTileWithKeyAndSequence(sequence: Int,
                                          key: String,
                                          zoom: Int,
                                          x: Int,
                                          y: Int,
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

  case class CountWithTileCoordinatesAndKey(count: Long, zoom: Int, x: Int, y: Int, key: String)

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
                                                                x: Int,
                                                                y: Int,
                                                                key: String)

  case class RasterTileWithKey(key: String,
                               zoom: Int,
                               x: Int,
                               y: Int,
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

  object RasterTileWithKey {
    def apply(key: String, zoom: Int, col: Int, row: Int, raster: Raster[Tile]): RasterTileWithKey =
      RasterTileWithKey(
        key,
        zoom,
        col,
        row,
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
              col: Int,
              row: Int,
              raster: Raster[Tile]): RasterTileWithKeyAndSequence =
      RasterTileWithKeyAndSequence(
        sequence,
        key,
        zoom,
        col,
        row,
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
