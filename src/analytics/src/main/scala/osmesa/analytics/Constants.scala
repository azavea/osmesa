package osmesa.analytics

object Constants {
  val ROAD_VALUES =
    Set(
      "motorway", "trunk", "motorway_link", "trunk_link", "primary", "secondary", "tertiary",
      "primary_link", "secondary_link", "tertiary_link", "service", "residential", "unclassified",
      "living_street", "road"
    )

  val WATERWAY_VALUES =
    Set(
      "river", "canal", "stream", "brook", "drain", "ditch"
    )
}
