package osmesa.analytics.data

import osmesa.analytics._

import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json._

object Geometries {
  def eaglesField: Polygon =
    Resource("countries.geojson").
      parseGeoJson[JsonFeatureCollection].
      getAll[Polygon].
      head
}
