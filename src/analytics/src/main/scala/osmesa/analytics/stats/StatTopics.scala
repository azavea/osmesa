package osmesa.analytics.stats

import osmesa.analytics.Constants

import scala.collection.mutable

object StatTopics {
  def ALL = List(BUILDING, ROAD, WATERWAY, POI)

  val BUILDING = "B"
  val ROAD = "R"
  val WATERWAY = "W"
  val POI = "P"

  def tagsToTopics(tags: Map[String, String]): Array[StatTopic] = {
    val b = mutable.ArrayBuffer[StatTopic]()
    tags.get("highway") match {
      case Some(v) if Constants.ROAD_VALUES.contains(v) => b += StatTopics.ROAD
      case _ => ()
    }

    tags.get("waterway") match {
      case Some(v) if Constants.WATERWAY_VALUES.contains(v) => b += StatTopics.WATERWAY
      case _ => ()
    }

    tags.get("building") match {
      case Some(v) if v.toLowerCase != "no" => b += StatTopics.BUILDING
      case _ => ()
    }

    if(tags contains "amenity") { b += StatTopics.POI }

    b.toArray
  }
}
