package osmesa.analytics

import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.prep.{PreparedGeometry, PreparedGeometryFactory}
import geotrellis.vector._
import geotrellis.vector.io.json._
import _root_.io.circe._
import _root_.io.circe.generic.semiauto._


case class CountryId(name: String, code: Short)
object CountryId {
  implicit val countryIdDecoder: Decoder[CountryId] = deriveDecoder
  implicit val countryIdEncoder: Encoder[CountryId] = deriveEncoder
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

  def byName: Map[String, MultiPolygonFeature[CountryId]] =
    all.map { f => (f.data.name, f) }.toMap

  def indexed: SpatialIndex[MultiPolygonFeature[CountryId]] =
    SpatialIndex.fromExtents(all) { mpf => mpf.geom.getEnvelopeInternal }

}

class CountryLookup() extends Serializable {
  private val index =
    SpatialIndex.fromExtents(
      Countries.all.
        map { mpf =>
          (PreparedGeometryFactory.prepare(mpf.geom), mpf.data)
        }
    ) { case (pg, _) => pg.getGeometry.getEnvelopeInternal }

  def lookup(coord: Coordinate): Option[CountryId] = {
    val t =
      new Traversable[(PreparedGeometry, CountryId)] {
        override def foreach[U](f: ((PreparedGeometry, CountryId)) => U): Unit = {
          val visitor = new org.locationtech.jts.index.ItemVisitor {
            override def visitItem(obj: AnyRef): Unit = f(obj.asInstanceOf[(PreparedGeometry, CountryId)])
          }
          index.rtree.query(new org.locationtech.jts.geom.Envelope(coord), visitor)
        }
      }

    t.
      find(_._1.covers(Point(coord.x, coord.y))).
      map(_._2)
  }
}
