package osmesa.common

import java.net.URI
import java.time.Instant

import cats.syntax.either._
import geotrellis.raster.{Tile, Raster => GTRaster}
import geotrellis.vector.io._
import geotrellis.vector.{Point, Geometry => GTGeometry}
import io.circe._

import scala.util.Random

package object model {
  implicit class RandomGetSeq[A](lst: Seq[A]) {
    def takeRandom: Option[A] = lst.lift(Random.nextInt(lst.size))
  }

  implicit class RandomGetArray[A](lst: Array[A]) {
    def takeRandom: Option[A] = lst.lift(Random.nextInt(lst.size))
  }

  implicit val encodeInstant: Encoder[Instant] =
    Encoder.encodeString.contramap[Instant](_.toString)

  implicit val decodeInstant: Decoder[Instant] = Decoder.decodeString.emap { str =>
    Either.catchNonFatal(Instant.parse(str)).leftMap(t => "Instant")
  }

  implicit val encodeURI: Encoder[URI] =
    Encoder.encodeString.contramap[URI](_.toString)

  implicit val decodeURI: Decoder[URI] = Decoder.decodeString.emap { str =>
    Either.catchNonFatal(new URI(str)).leftMap(t => "URI")
  }

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
}
