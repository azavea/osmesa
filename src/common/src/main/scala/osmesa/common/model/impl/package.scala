package osmesa.common.model

import geotrellis.raster.{ArrayTile, CellType, IntArrayTile, MutableArrayTile, Tile, isData, Raster => GTRaster}
import geotrellis.vector.Extent
import osmesa.common.raster.{MutableSparseIntTile, SparseIntTile}

package object impl {
  case class CoordinatesWithKey(key: String, lat: Option[BigDecimal], lon: Option[BigDecimal])
      extends Coordinates
      with Key

  import implicits._

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

  case class GeometryTileWithKey(key: String, zoom: Int, x: Int, y: Int, wkb: Array[Byte])
      extends GeometryTile
      with Key

  case class GeometryTileWithKeyAndSequence(sequence: Int,
                                            key: String,
                                            zoom: Int,
                                            x: Int,
                                            y: Int,
                                            wkb: Array[Byte])
      extends GeometryTile
      with Key
      with Sequence

  case class RasterTileWithKey(key: String,
                               zoom: Int,
                               x: Int,
                               y: Int,
                               tileValues: Map[Long, Int],
                               tileCols: Int,
                               tileRows: Int,
                               tileCellType: String,
                               xmin: Double,
                               ymin: Double,
                               xmax: Double,
                               ymax: Double)
      extends RasterTile
      with Key {
    lazy val raster: GTRaster[Tile] = GTRaster.tupToRaster(
      SparseIntTile(tileCols, tileRows, tileValues),
      Extent(xmin, ymin, xmax, ymax)
    )

    override def prototype(cols: Int, rows: Int): MutableArrayTile =
      ArrayTile.empty(cellType, tileCols, tileRows)

    def cellType: CellType = CellType.fromName(tileCellType)
  }

  case class RasterTileWithKeyAndSequence(sequence: Int,
                                          key: String,
                                          zoom: Int,
                                          x: Int,
                                          y: Int,
                                          tileValues: Map[Long, Int],
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
      SparseIntTile(tileCols, tileRows, tileValues),
      Extent(xmin, ymin, xmax, ymax)
    )

    override def prototype(cols: Int, rows: Int): MutableArrayTile =
      ArrayTile.empty(cellType, tileCols, tileRows)

    def cellType: CellType = CellType.fromName(tileCellType)
  }

  case class RasterWithSequenceTileSeqWithTileCoordinatesAndKey(tiles: Seq[RasterWithSequence],
                                                                zoom: Int,
                                                                x: Int,
                                                                y: Int,
                                                                key: String)
      extends RasterWithSequenceTileSeq
      with TileCoordinates
      with Key

  case class RasterWithSequence(tileValues: Map[Long, Int],
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
      SparseIntTile(tileCols, tileRows, tileValues),
      Extent(xmin, ymin, xmax, ymax)
    )

    override def prototype(cols: Int, rows: Int): MutableArrayTile =
      ArrayTile.empty(cellType, tileCols, tileRows)

    def cellType: CellType = CellType.fromName(tileCellType)
  }

  case class CountWithTileCoordinatesAndKey(count: Long, zoom: Int, x: Int, y: Int, key: String)
      extends Count
      with TileCoordinates
      with Key

  object implicits {
    implicit class RasterMethods(val raster: GTRaster[Tile]) {
      def toMap: Map[Long, Int] = {
        raster.tile match {
          case tile: SparseIntTile        => tile.toMap
          case tile: MutableSparseIntTile => tile.toMap
          case tile =>
            tile
              .toArray()
              .zipWithIndex
              .filter(x => isData(x._1))
              .map(x => (x._2.toLong, x._1))
              .toMap
        }
      }
    }
  }

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

  object RasterTileWithKey {
    def apply(key: String,
              zoom: Int,
              col: Int,
              row: Int,
              raster: GTRaster[Tile]): RasterTileWithKey =
      RasterTileWithKey(
        key,
        zoom,
        col,
        row,
        raster.toMap,
        raster.cols,
        raster.rows,
        raster.cellType.name,
        raster.extent.xmin,
        raster.extent.ymin,
        raster.extent.xmax,
        raster.extent.ymax
      )
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
        raster.toMap,
        raster.cols,
        raster.rows,
        raster.cellType.name,
        raster.extent.xmin,
        raster.extent.ymin,
        raster.extent.xmax,
        raster.extent.ymax
      )
  }

  IntArrayTile

  object RasterWithSequence {
    def apply(raster: GTRaster[Tile], sequence: Int): RasterWithSequence =
      RasterWithSequence(
        raster.toMap,
        raster.cols,
        raster.rows,
        raster.cellType.name,
        raster.extent.xmin,
        raster.extent.ymin,
        raster.extent.xmax,
        raster.extent.ymax,
        sequence
      )
  }
}
