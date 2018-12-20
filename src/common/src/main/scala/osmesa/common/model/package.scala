package osmesa.common

import com.vividsolutions.jts.{geom => jts}
import geotrellis.raster.{Tile, Raster => GTRaster}
import geotrellis.vector.io._
import geotrellis.vector.{Point, Geometry => GTGeometry}

package object model {
  trait Geometry {
    def geom: jts.Geometry
  }

  trait SerializedGeometry extends Geometry {
    def geom: jts.Geometry

    lazy val wkb: Array[Byte] = (new com.vividsolutions.jts.io.WKBWriter).write(geom)
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
    def lat: Option[Double]
    def lon: Option[Double]

    def geom: jts.Point = {
      val gf = new jts.GeometryFactory
      gf.createPoint(new jts.Coordinate(x, y))
    }

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
}
