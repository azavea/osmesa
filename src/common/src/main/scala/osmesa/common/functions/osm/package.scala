package osmesa.common.functions

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}
import osmesa.common.ProcessOSM._
import osmesa.common.util._

import scala.util.matching.Regex

package object osm {
  // Using tag listings from [id-area-keys](https://github.com/osmlab/id-area-keys) @ v2.13.0.
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
      "summer_toboggan" -> true,
      "train" -> true,
      "water_slide" -> true
    ),
    "bridge:support" -> Map(),
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
    "internet_access" -> Map(),
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
    "piste:type" -> Map(
      "downhill" -> true,
      "hike" -> true,
      "ice_skate" -> true,
      "nordic" -> true,
      "skitour" -> true,
      "sled" -> true,
      "sleigh" -> true
    ),
    "place" -> Map(),
    "playground" -> Map(
      "balancebeam" -> true,
      "slide" -> true,
      "zipwire" -> true
    ),
    "power" -> Map(
      "cable" -> true,
      "line" -> true,
      "minor_line" -> true
    ),
    "public_transport" -> Map(
      "platform" -> true
    ),
    "residential" -> Map(),
    "seamark:type" -> Map(),
    "shop" -> Map(),
    "tourism" -> Map(
      "artwork" -> true
    ),
    "traffic_calming" -> Map(
      "bump" -> true,
      "cushion" -> true,
      "dip" -> true,
      "hump" -> true,
      "rumble_strip" -> true
    ),
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

  private val MultiPolygonTypes = Seq("multipolygon")

  private val BooleanValues = Seq("yes", "no", "true", "false", "1", "0")

  private val TruthyValues = Seq("yes", "true", "1")

  private val WaterwayValues =
    Seq(
      "river", "canal", "stream", "brook", "drain", "ditch"
    )

  private val POITags = Set("amenity", "shop", "craft", "office", "leisure", "aeroway")

  private val HashtagMatcher: Regex = """#([^\u2000-\u206F\u2E00-\u2E7F\s\\'!\"#$%()*,.\/;<=>?@\[\]^{|}~]+)""".r

  private val _isArea = (tags: Map[String, String]) =>
    tags match {
      case _ if tags.contains("area") && BooleanValues.intersect(tags("area").toLowerCase.split(";").map(_.trim)).nonEmpty =>
        TruthyValues.contains(tags("area").toLowerCase)
      case _ =>
        // see https://github.com/osmlab/id-area-keys (values are inverted)
        val matchingKeys = tags.keySet.intersect(AreaKeys.keySet)
        matchingKeys.exists(k =>
          // values that should be considered as lines
          AreaKeys(k).keySet
            .intersect(
              // break out semicolon-delimited values
              tags(k).toLowerCase().split(";").map(_.trim).toSet
            )
            .isEmpty
        )
    }

  val isAreaUDF: UserDefinedFunction = udf(_isArea)

  def isArea(tags: Column): Column = isAreaUDF(tags) as 'isArea

  def isMultiPolygon(tags: Column): Column =
    array_intersects(
      split(
        lower(
          coalesce(
            regexp_replace(trim(tags.getItem("type")), ";\\s+", ";"),
            lit(""))),
        ";"),
      lit(MultiPolygonTypes.toArray)) as 'isMultiPolygon

  def isNew(version: Column, minorVersion: Column): Column =
    version <=> 1 && minorVersion <=> 0 as 'isNew

  def isRoute(tags: Column): Column =
    array_contains(split(lower(coalesce(regexp_replace(trim(tags.getItem("type")), ";\\s+", ";"), lit(""))), ";"), "route") as 'isRoute

  private lazy val MemberSchema = ArrayType(
    StructType(
      StructField("type", ByteType, nullable = false) ::
        StructField("ref", LongType, nullable = false) ::
        StructField("role", StringType, nullable = false) ::
        Nil), containsNull = false)

  private lazy val UncompressedMemberSchema = ArrayType(
    StructType(
      StructField("type", StringType, nullable = false) ::
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

  lazy val compressMemberTypes: UserDefinedFunction = udf(_compressMemberTypes, MemberSchema)

  private val _uncompressMemberTypes = (members: Seq[Row]) =>
    members.map { row =>
      val t = row.getAs[Byte]("type") match {
        case NodeType => "node"
        case WayType => "way"
        case RelationType => "relation"
      }
      val ref = row.getAs[Long]("ref")
      val role = row.getAs[String]("role")

      Row(t, ref, role)
    }

  lazy val uncompressMemberTypes: UserDefinedFunction = udf(_uncompressMemberTypes, UncompressedMemberSchema)

  // matches letters or emoji (no numbers or punctuation)
  private val ContentMatcher: Regex = """[\p{L}\uD83C-\uDBFF\uDC00-\uDFFF]""".r
  private val TrailingPunctuationMatcher: Regex = """[:]$""".r

  val extractHashtags: UserDefinedFunction = udf { comment: String =>
    HashtagMatcher
      .findAllMatchIn(comment)
      // fetch the first group (after #)
      .map(_.group(1).toLowerCase)
      // check that each group contains at least one substantive character
      .filter(ContentMatcher.findFirstIn(_).isDefined)
      // strip trailing punctuation
      .map(TrailingPunctuationMatcher.replaceAllIn(_, ""))
      .toList // prevent a Stream from being returned
      .distinct
  }

  def hashtags(comment: Column): Column =
    // only call the UDF when necessary
    when(comment.isNotNull and length(comment) > 0, extractHashtags(comment))
      .otherwise(typedLit(Seq.empty[String])) as 'hashtags

  def isTagged(tags: Column): Column = size(map_keys(tags)) > 0 as 'isTagged

  def isBuilding(tags: Column): Column =
    !array_contains(split(lower(coalesce(regexp_replace(trim(tags.getItem("building")), ";\\s+", ";"), lit("no"))), ";"), "no") as 'isBuilding

  val isPOI: UserDefinedFunction = udf {
    tags: Map[String, String] => POITags.intersect(tags.keySet).nonEmpty
  }

  def isRoad(tags: Column): Column =
    tags.getItem("highway").isNotNull as 'isRoad

  def isCoastline(tags: Column): Column =
    array_contains(split(lower(coalesce(regexp_replace(trim(tags.getItem("natural")), ";\\s+", ";"), lit(""))), ";"), "coastline") as 'isCoastline

  def isWaterway(tags: Column): Column =
    array_intersects(split(lower(coalesce(regexp_replace(trim(tags.getItem("waterway")), ";\\s+", ";"), lit(""))), ";"), lit(WaterwayValues.toArray)) as 'isWaterway

  def mergeTags: UserDefinedFunction = udf { (a: Map[String, String], b: Map[String, String]) =>
    mergeMaps(a.mapValues(Set(_)), b.mapValues(Set(_)))(_ ++ _).mapValues(_.mkString(";"))
  }

  val reduceTags: UserDefinedFunction = udf { tags: Iterable[Map[String, String]] =>
    tags.map(x => x.mapValues(Set(_))).reduce((a, b) => mergeMaps(a, b)(_ ++ _)).mapValues(_.mkString(";"))
  }
}
