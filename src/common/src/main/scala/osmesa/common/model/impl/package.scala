package osmesa.common.model

import geotrellis.raster.{
  CellType,
  IntArrayTile,
  IntCells,
  NoDataHandling,
  Tile,
  Raster => GTRaster
}
import geotrellis.vector.Extent

package object impl {
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
