package osmesa.ingest

import vectorpipe._
import vectorpipe.osm.OSMFeature
import geotrellis.vector.Line


object TagFilters {
  def isRoad(feature: OSMFeature): Boolean = {
    val ROAD_TAGS =
      Set(
        "motorway",
        "trunk",
        "motorway_link",
        "trunk_link",
        "primary",
        "secondary",
        "tertiary",
        "primary_link",
        "secondary_link",
        "tertiary_link",
        "service",
        "residential",
        "unclassified",
        "living_street",
        "road"
      )

    feature.geom match {
      case _: Line =>
        feature.data.tagMap.get("highway") match {
          case Some(t) if ROAD_TAGS.contains(t) => true
          case _ => false
        }
      case _ => false
    }
  }
}

