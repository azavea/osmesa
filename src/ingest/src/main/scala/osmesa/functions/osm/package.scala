package osmesa.functions

import java.sql.Timestamp

import geotrellis.vector.io._
import geotrellis.vector.{Line, MultiPolygon, Polygon}
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.annotation.tailrec

package object osm {
  // Using tag listings from [id-area-keys](https://github.com/osmlab/id-area-keys).
  private val AREA_KEYS: Map[String, Map[String, Boolean]] = Map(
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

  private lazy val logger = Logger.getRootLogger

  private val _isArea = (tags: Map[String, String]) =>
    tags match {
      case _ if tags.contains("area") && Set("yes", "no", "true", "1").contains(tags("area").toLowerCase) =>
        Set("yes", "true", "1").contains(tags("area").toLowerCase)
      case _ =>
        // see https://github.com/osmlab/id-area-keys (values are inverted)
        val matchingKeys = tags.keySet.intersect(AREA_KEYS.keySet)
        matchingKeys.exists(k => !AREA_KEYS(k).contains(tags(k)))
    }

  val isArea: UserDefinedFunction = udf(_isArea)

  private val _isMultiPolygon = (tags: Map[String, String]) =>
    tags.contains("type") && Set("boundary", "multipolygon").contains(tags("type").toLowerCase)

  val isMultiPolygon: UserDefinedFunction = udf(_isMultiPolygon)

  // create fully-formed rings from line segments
  @tailrec
  private def connectSegments(segments: List[Line], rings: List[Polygon] = List.empty[Polygon]): List[Polygon] = {
    segments match {
      case Nil => rings
      case h :: t if h.isClosed => connectSegments(t, rings :+ Polygon(h))
      case h :: t =>
        connectSegments(t.find(line => h.vertices.last == line.vertices.head) match {
          case Some(next) => Line(h.vertices ++ next.vertices.tail) :: t.filterNot(line => line == next)
          case None =>
            t.find(line => h.vertices.last == line.vertices.last) match {
              case Some(next) => Line(h.vertices ++ next.vertices.reverse.tail) :: t.filterNot(line => line == next)
              case None => throw new Exception("Unable to connect segments.")
            }
        }, rings)
    }
  }

  @tailrec
  private def dissolveRings(rings: List[Polygon], dissolvedOuters: List[Polygon] = List.empty[Polygon], dissolvedInners: List[Polygon] = List.empty[Polygon]): (List[Polygon], List[Polygon]) = {
    rings match {
      case Nil => (dissolvedOuters, dissolvedInners)
      case h :: t =>
        t.filter(r => h.touches(r)) match {
          case touching if touching.isEmpty => dissolveRings(t.filterNot(r => h.touches(r)), dissolvedOuters :+ Polygon(h.exterior), dissolvedInners ++ h.holes.map(Polygon(_)))
          case touching =>
            val dissolved = touching.foldLeft(List(h)) {
              case (rs, r2) =>
                rs.flatMap { r =>
                  r.union(r2).toGeometry match {
                    case Some(p: Polygon) => List(p)
                    case Some(mp: MultiPolygon) => mp.polygons
                    case _ => throw new Exception("Union failed.")
                  }
                }
            }

            val remaining = t.filterNot(r => h.touches(r))
            val retryRings = dissolved.filter(d => remaining.exists(r => r.touches(d)))
            val newRings = dissolved.filter(d => !remaining.exists(r => r.touches(d)))

            dissolveRings(retryRings ++ remaining, dissolvedOuters ++ newRings.map(_.exterior).map(Polygon(_)), dissolvedInners ++ newRings.flatMap(_.holes).map(Polygon(_)))
        }
    }
  }

  // TODO type=route relations
  // TODO filter relations referring to other relations
  // TODO remove ways without unique tags that participate in multipolygon relations

  val buildMultiPolygon: UserDefinedFunction = udf((ways: Seq[Row], id: Long, version: Long, timestamp: Timestamp) => {
    try {
      if (ways.exists(row => row.getAs[String]("type") == "way" && Option(row.getAs[Array[Byte]]("geom")).isEmpty)) {
        // bail early if null values are present where they should exist (members w/ type=way)
        logger.debug(s"Incomplete relation: $id @ $version ($timestamp)")
        null
      } else if (ways.forall(row => Option(row.getAs[String]("type")).isEmpty && Option(row.getAs[Array[Byte]]("geom")).isEmpty)) {
        // all members are null
        null
      } else {
        val coords: Seq[(String, Line)] = ways
          .map(row =>
            (row.getAs[String]("role"), Option(row.getAs[Array[Byte]]("geom")).map(_.readWKB) match {
              case Some(geom: Polygon) => geom.as[Polygon].map(_.exterior)
              case Some(geom) => geom.as[Line]
              case None => None
            }))
          .filter(_._2.isDefined)
          .map(x => (x._1, x._2.get))

        val (completeOuters, completeInners, completeUnknowns, partialOuters, partialInners, partialUnknowns) = coords.foldLeft((List.empty[Polygon], List.empty[Polygon], List.empty[Polygon], List.empty[Line], List.empty[Line], List.empty[Line])) {
          case ((co, ci, cu, po, pi, pu), (role, geom: Line)) =>
            Option(geom) match {
              case Some(line) =>
                role match {
                  case "outer" if line.isClosed => (co :+ Polygon(line), ci, cu, po, pi, pu)
                  case "outer" => (co, ci, cu, po :+ line, pi, pu)
                  case "inner" if line.isClosed => (co, ci :+ Polygon(line), cu, po, pi, pu)
                  case "inner" => (co, ci, cu, po, pi :+ line, pu)
                  case "" if line.isClosed => (co, ci, cu :+ Polygon(line), po, pi, pu)
                  case "" => (co, ci, cu, po, pi, pu :+ line)
                  case _ => (co, ci, cu, po, pi, pu)
                }
              case None => (co, ci, cu, po, pi, pu)
            }
        }

        val unknowns: List[Polygon] = completeUnknowns ++ connectSegments(partialUnknowns.sortWith(_.length > _.length))

        val (outers, inners) = unknowns.foldLeft((completeOuters ++ connectSegments(partialOuters.sortWith(_.length > _.length)), completeInners ++ connectSegments(partialInners.sortWith(_.length > _.length)))) {
          case ((o: List[Polygon], i: List[Polygon]), u) =>
            if (o.exists(_.contains(u))) {
              (o, i :+ u)
            } else {
              (o :+ u, i)
            }
        }

        // reclassify rings according to their topology (ignoring roles)
        // TODO this isn't quite right: island in lake on island in lake (inner in outer in inner in outer)
        val (classifiedOuters, classifiedInners) = (outers ++ inners).sortWith(_.area > _.area) match {
          case h :: t => t.foldLeft((List(h), List.empty[Polygon])) {
            case ((os, is), ring) =>
              ring match {
                // there's an inner ring that contains this one; this is an outer ring
                case _ if is.exists(r => r.contains(ring)) => (os :+ ring, is)
                // there's an outer ring that contains this one; this is an inner ring
                case _ if os.exists(r => r.contains(ring)) => (os, is :+ ring)
                case _ => (os :+ ring, is)
              }
          }
          case Nil => (List.empty[Polygon], List.empty[Polygon])
        }

        val (dissolvedOuters, addlInners) = dissolveRings(classifiedOuters)
        val (dissolvedInners, addlOuters) = dissolveRings(classifiedInners.map(_.exterior).map(Polygon(_)) ++ addlInners)

        val (polygons, _) = (dissolvedOuters ++ addlOuters)
          // sort by size (descending) to use rings as part of the largest available polygon
          .sortWith(_.area > _.area)
          // only use inners once if they're contained by multiple outer rings
          .foldLeft((List.empty[Polygon], dissolvedInners)) {
          case ((ps, is), (outer)) =>
            (ps :+ Polygon(outer.exterior, is.filter(inner => outer.contains(inner)).map(_.exterior)), is.filterNot(inner => outer.contains(inner)))
        }

        polygons match {
          case p :: Nil => p.toWKB(4326)
          case ps => MultiPolygon(ps).toWKB(4326)
        }
      }
    } catch {
      case e: Throwable =>
        logger.warn(s"Could not reconstruct relation $id @ $version ($timestamp): $e")
        null
    }
  })
}
