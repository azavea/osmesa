package osmesa

package object analytics extends ColumnFunctions {
  type StatTopic = String

  object StatTopics {
    val BUILDING = "B"
    val ROAD = "R"
    val WATERWAY = "W"
    val POI = "P"
  }

}
