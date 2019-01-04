package osmesa.common.functions

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import osmesa.common.ProcessOSM._

package object osm {
  // Using tag listings from [id-area-keys](https://github.com/osmlab/id-area-keys) @ v2.8.0.
  private val AreaKeys: Map[String, Map[String, Boolean]] = Map(
    "addr:*" -> Map(),
    "advertising" -> Map(
      "billboard" -> true
    ),
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
    "allotments" -> Map(),
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
      "circular" -> true,
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

  private val MultiPolygonTypes = Set("multipolygon", "boundary")

  private val BooleanValues = Set("yes", "no", "true", "false", "1", "0")

  private val TruthyValues = Set("yes", "true", "1")

  private val WaterwayValues =
    Set(
      "river", "canal", "stream", "brook", "drain", "ditch"
    )

  private val POITags = Set("amenity", "shop", "craft", "office", "leisure", "aeroway")

  private val HashtagMatcher = """#([^\u2000-\u206F\u2E00-\u2E7F\s\\'!"#$%()*,.\/;<=>?@\[\]^{|}~]+)""".r

  private val _isArea = (tags: Map[String, String]) =>
    tags match {
      case _ if tags.contains("area") && BooleanValues.contains(tags("area").toLowerCase) =>
        TruthyValues.contains(tags("area").toLowerCase)
      case _ =>
        // see https://github.com/osmlab/id-area-keys (values are inverted)
        val matchingKeys = tags.keySet.intersect(AreaKeys.keySet)
        matchingKeys.exists(k => !AreaKeys(k).contains(tags(k)))
    }

  val isArea: UserDefinedFunction = udf(_isArea)

  private val _isMultiPolygon = (tags: Map[String, String]) =>
    tags.contains("type") && MultiPolygonTypes.contains(tags("type").toLowerCase)

  val isMultiPolygon: UserDefinedFunction = udf(_isMultiPolygon)

  val isNew: UserDefinedFunction = udf { (version: Int, minorVersion: Int) =>
    version == 1 && minorVersion == 0
  }

  val isRoute: UserDefinedFunction = udf { tags: Map[String, String] =>
    tags.contains("type") && tags("type") == "route"
  }

  private val MemberSchema = ArrayType(
    StructType(
      StructField("type", ByteType, nullable = false) ::
        StructField("ref", LongType, nullable = false) ::
        StructField("role", StringType, nullable = false) ::
        Nil), containsNull = false)

  private val _compressMemberTypes = (members: Seq[Row]) =>
    members.map { row =>
      val t = row.getAs[String]("type") match {
        case "node" => NodeType
        case "way" => WayType
        case "relation" => RelationType
      }
      val ref = row.getAs[Long]("ref")
      val role = row.getAs[String]("role")

      Row(t, ref, role)
    }

  val compressMemberTypes: UserDefinedFunction = udf(_compressMemberTypes, MemberSchema)

  private val _hashtags = (comment: String) =>
    HashtagMatcher
      .findAllMatchIn(comment)
      // fetch the first group (after #)
      .map(_.group(1).toLowerCase)
      // check that each group contains at least one letter
      .filter("""\p{L}""".r.findFirstIn(_).isDefined)
      .toSeq

  val hashtags: UserDefinedFunction = udf { tags: Map[String, String] =>
    tags.get("comment") match {
      case Some(comment) => _hashtags(comment)
      case None => Seq.empty[String]
    }
  }

  val isBuilding: UserDefinedFunction = udf {
    (_: Map[String, String]).getOrElse("building", "no").toLowerCase != "no"
  }

  val isPOI: UserDefinedFunction = udf {
    tags: Map[String, String] => POITags.intersect(tags.keySet).nonEmpty
  }

  val isRoad: UserDefinedFunction = udf {
    (_: Map[String, String]).contains("highway")
  }

  val isWaterway: UserDefinedFunction = udf {
    tags: Map[String, String] => WaterwayValues.contains(tags.getOrElse("waterway", null))
  }


  def mergeTags: UserDefinedFunction = udf {
    (_: Map[String, String]) ++ (_: Map[String, String])
  }
}
