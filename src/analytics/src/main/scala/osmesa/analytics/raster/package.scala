package osmesa.analytics
import geotrellis.raster.{Raster, Tile, isData}

package object raster {
  implicit class RasterMethods(val raster: Raster[Tile]) {
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
