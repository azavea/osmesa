package osmesa.analytics.oneoffs

import geotrellis.raster.{
  CellType,
  IntArrayTile,
  IntCells,
  NoDataHandling,
  Tile,
  Raster => GTRaster
}
import geotrellis.vector.io._
import geotrellis.vector.{Extent, Point, Geometry => GTGeometry}

// this is a placeholder package until such point at which much of this is moved to core (with case classes in an impl
// package)
package object traits {
  trait Geometry {
    def geom: GTGeometry
  }

  trait SerializedGeometry extends Geometry {
    lazy val geom: GTGeometry = wkb.readWKB

    def wkb: Array[Byte]
  }

  trait TileCoordinates {
    def zoom: Int
    def x: Int
    def y: Int
  }

  trait GeometryTile extends SerializedGeometry with TileCoordinates

  trait Raster {
    def raster: GTRaster[Tile]
  }

  trait RasterTile extends Raster with TileCoordinates

  trait Coordinates extends Geometry {
    def lat: Option[BigDecimal]
    def lon: Option[BigDecimal]

    def geom: Point = Point(x, y)

    def x: Float = lon.map(_.floatValue).getOrElse(Float.NaN)
    def y: Float = lat.map(_.floatValue).getOrElse(Float.NaN)
  }

  trait Key {
    def key: String
  }

  trait Sequence {
    def sequence: Int
  }

  // NOTE this doesn't extend TileSeq[T] to avoid using type parameters
  trait RasterWithSequenceTileSeq {
    def tiles: Seq[Raster with Sequence]
  }

  trait Count {
    def count: Long
  }

  case class CoordinatesWithKeyAndSequence(sequence: Int,
                                           key: String,
                                           lat: Option[BigDecimal],
                                           lon: Option[BigDecimal])
      extends Coordinates
      with Key
      with Sequence

  case class TileCoordinatesWithKeyAndSequence(sequence: Int,
                                               key: String,
                                               zoom: Int,
                                               x: Int,
                                               y: Int)
      extends TileCoordinates
      with Key
      with Sequence

  case class GeometryTileWithKeyAndSequence(sequence: Int,
                                            key: String,
                                            zoom: Int,
                                            x: Int,
                                            y: Int,
                                            wkb: Array[Byte])
      extends GeometryTile
      with Key
      with Sequence

  case class RasterTileWithKeyAndSequence(sequence: Int,
                                          key: String,
                                          zoom: Int,
                                          x: Int,
                                          y: Int,
                                          tileBytes: Array[Byte],
                                          tileCols: Int,
                                          tileRows: Int,
                                          tileCellType: String,
                                          xmin: Double,
                                          ymin: Double,
                                          xmax: Double,
                                          ymax: Double)
      extends RasterTile
      with Key
      with Sequence {
    lazy val raster: GTRaster[Tile] = GTRaster.tupToRaster(
      IntArrayTile.fromBytes(
        tileBytes,
        tileCols,
        tileRows,
        CellType.fromName(tileCellType).asInstanceOf[IntCells with NoDataHandling]),
      Extent(xmin, ymin, xmax, ymax)
    )
  }

  case class RasterWithSequenceTileSeqWithTileCoordinatesAndKey(tiles: Seq[RasterWithSequence],
                                                                zoom: Int,
                                                                x: Int,
                                                                y: Int,
                                                                key: String)
      extends RasterWithSequenceTileSeq
      with TileCoordinates
      with Key

  case class RasterWithSequence(tileBytes: Array[Byte],
                                tileCols: Int,
                                tileRows: Int,
                                tileCellType: String,
                                xmin: Double,
                                ymin: Double,
                                xmax: Double,
                                ymax: Double,
                                sequence: Int)
      extends Raster
      with Sequence {
    lazy val raster: GTRaster[Tile] = GTRaster.tupToRaster(
      IntArrayTile.fromBytes(
        tileBytes,
        tileCols,
        tileRows,
        CellType.fromName(tileCellType).asInstanceOf[IntCells with NoDataHandling]),
      Extent(xmin, ymin, xmax, ymax)
    )
  }

  case class CountWithTileCoordinatesAndKey(count: Long, zoom: Int, x: Int, y: Int, key: String)
      extends Count
      with TileCoordinates
      with Key

  object RasterWithSequenceTileSeqWithTileCoordinatesAndKey {
    def apply(tiles: Seq[Raster with Sequence], zoom: Int, x: Int, y: Int, key: String)(
        implicit d: DummyImplicit): RasterWithSequenceTileSeqWithTileCoordinatesAndKey =
      RasterWithSequenceTileSeqWithTileCoordinatesAndKey(
        // potentially unsafe cast assuming multiple implementations of Raster with Sequence
        tiles.map(_.asInstanceOf[RasterWithSequence]),
        zoom,
        x,
        y,
        key)
  }

  object RasterTileWithKeyAndSequence {
    def apply(sequence: Int,
              key: String,
              zoom: Int,
              col: Int,
              row: Int,
              raster: GTRaster[Tile]): RasterTileWithKeyAndSequence =
      RasterTileWithKeyAndSequence(
        sequence,
        key,
        zoom,
        col,
        row,
        raster.tile.toBytes,
        raster.tile.cols,
        raster.tile.rows,
        raster.tile.cellType.name,
        raster.extent.xmin,
        raster.extent.ymin,
        raster.extent.xmax,
        raster.extent.ymax
      )
  }

  object RasterWithSequence {
    def apply(raster: GTRaster[Tile], sequence: Int): RasterWithSequence =
      RasterWithSequence(
        raster.tile.toBytes,
        raster.tile.cols,
        raster.tile.rows,
        raster.tile.cellType.name,
        raster.extent.xmin,
        raster.extent.ymin,
        raster.extent.xmax,
        raster.extent.ymax,
        sequence
      )
  }
}
