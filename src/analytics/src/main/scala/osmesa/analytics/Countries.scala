package osmesa.analytics

import com.vividsolutions.jts.geom.Coordinate
import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json._
import spray.json._


case class CountryId(name: String, code: Short)

object CountryId {
  implicit object CountryIdJsonFormat extends RootJsonFormat[CountryId] {
    def read(value: JsValue): CountryId =
      value.asJsObject.getFields("NAME", "ISO_N3") match {
        case Seq(JsString(name), JsString(code)) =>
          CountryId(name, code.toShort)
        case v =>
          throw new DeserializationException(s"CountryId expected, got $v")
      }

    def write(v: CountryId): JsValue =
      JsObject(
        "name" -> JsString(v.name),
        "isoCode" -> JsNumber(v.code)
      )
  }
}

object Countries {
  def all: Vector[MultiPolygonFeature[CountryId]] = {
    val collection =
      Resource("countries.geojson").
        parseGeoJson[JsonFeatureCollection]

    val polys =
      collection.
        getAllPolygonFeatures[CountryId].
        map(_.mapGeom(MultiPolygon(_)))

    val mps =
      collection.
        getAllMultiPolygonFeatures[CountryId]

    polys ++ mps
  }

  def indexed: SpatialIndex[MultiPolygonFeature[CountryId]] =
    SpatialIndex.fromExtents(all) { mpf => mpf.geom.envelope }

}

class CountryLookup() extends Serializable {
  //private val index = Countries.indexed
  private val index =
    SpatialIndex.fromExtents(
      Countries.all.
        map { mpf =>
          (mpf.geom.prepare, mpf.data)
        }
    ) { case (pg, _) => pg.geom.envelope }

  def lookup(coord: Coordinate): Option[CountryId] = {
    val t =
      new Traversable[(prepared.PreparedGeometry[MultiPolygon], CountryId)] {
        override def foreach[U](f: ((prepared.PreparedGeometry[MultiPolygon], CountryId)) => U): Unit = {
          val visitor = new com.vividsolutions.jts.index.ItemVisitor {
            override def visitItem(obj: AnyRef): Unit = f(obj.asInstanceOf[(prepared.PreparedGeometry[MultiPolygon], CountryId)])
          }
          index.rtree.query(new com.vividsolutions.jts.geom.Envelope(coord), visitor)
        }
      }

    t.
      find(_._1.covers(Point(coord.x, coord.y))).
      map(_._2)
  }
}
