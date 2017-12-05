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

    if(tags contains "building") { b += StatTopics.BUILDING }
    if(tags contains "waterway") { b += StatTopics.WATERWAY }
    if(tags contains "amenity") { b += StatTopics.POI }

    b.toArray
  }
}
