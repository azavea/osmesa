package osmesa

object Constants {

    // Define `isArea` UDF
    // Using tag listings from [id-area-keys](https://github.com/osmlab/id-area-keys).
    val AREA_KEYS: Map[String, Map[String, Boolean]] = Map(
      "addr:*" -> Map(),
      "aerialway" -> Map(
        "cable_car" -> true,
        "chair_lift" -> true,
        "drag_lift" -> true,
        "gondola" -> true,
        "goods" -> true,
        "magic_carpet" -> true,
        "mixed_lift" -> true,
        "platter" -> true,
        "rope_tow" -> true,
        "t-bar" -> true
      ),
      "aeroway" -> Map(
        "runway" -> true,
        "taxiway" -> true
      ),
      "amenity" -> Map(
        "bench" -> true
      ),
      "area:highway" -> Map(),
      "attraction" -> Map(
        "dark_ride" -> true,
        "river_rafting" -> true,
        "train" -> true,
        "water_slide" -> true
      ),
      "building" -> Map(),
      "camp_site" -> Map(),
      "club" -> Map(),
      "craft" -> Map(),
      "emergency" -> Map(
        "designated" -> true,
        "destination" -> true,
        "no" -> true,
        "official" -> true,
        "private" -> true,
        "yes" -> true
      ),
      "golf" -> Map(
        "hole" -> true,
        "lateral_water_hazard" -> true,
        "water_hazard" -> true
      ),
      "healthcare" -> Map(),
      "historic" -> Map(),
      "industrial" -> Map(),
      "junction" -> Map(
        "roundabout" -> true
      ),
      "landuse" -> Map(),
      "leisure" -> Map(
        "slipway" -> true,
        "track" -> true
      ),
      "man_made" -> Map(
        "breakwater" -> true,
        "crane" -> true,
        "cutline" -> true,
        "embankment" -> true,
        "groyne" -> true,
        "pier" -> true,
        "pipeline" -> true
      ),
      "military" -> Map(),
      "natural" -> Map(
        "cliff" -> true,
        "coastline" -> true,
        "ridge" -> true,
        "tree_row" -> true
      ),
      "office" -> Map(),
      "piste:type" -> Map(),
      "place" -> Map(),
      "playground" -> Map(
        "balancebeam" -> true,
        "slide" -> true,
        "zipwire" -> true
      ),
      "power" -> Map(
        "line" -> true,
        "minor_line" -> true
      ),
      "public_transport" -> Map(
        "platform" -> true
      ),
      "shop" -> Map(),
      "tourism" -> Map(),
      "waterway" -> Map(
        "canal" -> true,
        "dam" -> true,
        "ditch" -> true,
        "drain" -> true,
        "river" -> true,
        "stream" -> true,
        "weir" -> true
      )
    )

}
