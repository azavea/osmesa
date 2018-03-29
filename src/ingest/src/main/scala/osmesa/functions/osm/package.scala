package osmesa.functions

import java.sql.Timestamp

import geotrellis.vector.{Line, MultiPolygon, Polygon}
import geotrellis.vector.io._
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import osmesa.Constants

import scala.annotation.tailrec

package object osm {
  private lazy val logger = Logger.getRootLogger

  private val _isArea = (tags: Map[String, String]) =>
    tags match {
      case _ if tags.contains("area") && Set("yes", "no", "true", "1").contains(tags("area").toLowerCase) =>
        Set("yes", "true", "1").contains(tags("area").toLowerCase)
      case _ =>
        // see https://github.com/osmlab/id-area-keys (values are inverted)
        val matchingKeys = tags.keySet.intersect(Constants.AREA_KEYS.keySet)
        matchingKeys.exists(k => !Constants.AREA_KEYS(k).contains(tags(k)))
    }

  val isArea: UserDefinedFunction = udf(_isArea)

  private val _isMultiPolygon = (tags: Map[String, String]) =>
    tags.contains("type") && Set("boundary", "multipolygon").contains(tags("type").toLowerCase)

  val isMultiPolygon: UserDefinedFunction = udf(_isMultiPolygon)

  // create fully-formed rings from line segments
  @tailrec
  private def connectSegments(segments: List[Line], rings: List[Line] = List.empty[Line]): List[Line] = {
    segments match {
      case Nil => rings
      case h :: t if h.isClosed => connectSegments(t, rings :+ h)
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
  private def dissolveRings(rings: List[Polygon], dissolvedOuters: List[Line] = List.empty[Line], dissolvedInners: List[Line] = List.empty[Line]): (List[Line], List[Line]) = {
    rings match {
      case Nil => (dissolvedOuters, dissolvedInners)
      case h :: t =>
        t.filter(r => h.touches(r)) match {
          case touching if touching.isEmpty => dissolveRings(t.filterNot(r => h.touches(r)), dissolvedOuters :+ h.exterior, dissolvedInners ++ h.holes)
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

            dissolveRings(retryRings ++ remaining, dissolvedOuters ++ newRings.map(_.exterior), dissolvedInners ++ newRings.flatMap(_.holes))
        }
    }
  }

  private def dissolveRings(rings: List[Line]): (List[Line], List[Line]) = {
    dissolveRings(rings.map(x => Polygon(x)))
  }

  // TODO type=route relations
  // TODO filter relations referring to other relations
  // TODO remove ways without unique tags that participate in multipolygon relations

  val buildMultiPolygon: UserDefinedFunction = udf((ways: Seq[Row], id: Long, version: Long, timestamp: Timestamp) => {
    try {
      // bail early if null values are present where they should exist (members w/ type=way)
      if (ways.exists(row => row.getAs[String]("type") == "way" && Option(row.getAs[Array[Byte]]("geom")).isEmpty)) {
        logger.debug(s"Incomplete relation: $id @ $version ($timestamp)")
        null
      } else {
        // TODO convert to Polygons initially to avoid repeated conversions
        val coords: Seq[(String, Line)] = ways
          .map(row => (row.getAs[String]("role"), Option(row.getAs[Array[Byte]]("geom")).map(_.readWKB) match {
            case Some(geom: Polygon) => geom.as[Polygon].map(_.exterior)
            case Some(geom) => geom.as[Line]
            case None => None
          })).filter(_._2.isDefined).map(x => (x._1, x._2.get))

        val (completeOuters, completeInners, partialOuters, partialInners, completeUnknowns, partialUnknowns) = coords.foldLeft((List.empty[Line], List.empty[Line], List.empty[Line], List.empty[Line], List.empty[Line], List.empty[Line])) {
          case ((co, ci, po, pi, cu, pu), (role, geom: Line)) =>
            Option(geom) match {
              case Some(line) =>
                role match {
                  case "outer" if line.isClosed => (co :+ line, ci, po, pi, cu, pu)
                  case "outer" => (co, ci, po :+ line, pi, cu, pu)
                  case "inner" if line.isClosed => (co, ci :+ line, po, pi, cu, pu)
                  case "inner" => (co, ci, po, pi :+ line, cu, pu)
                  case "" if line.isClosed => (co, ci, po, pi, cu :+ line, pu)
                  case "" => (co, ci, po, pi, cu, pu :+ line)
                  case _ => (co, ci, po, pi, cu, pu)
                }
              case None => (co, ci, po, pi, cu, pu)
            }
        }

        val unknowns: List[Line] = completeUnknowns ++ connectSegments(partialUnknowns.sortWith(_.length > _.length))

        val (outers, inners) = unknowns.foldLeft((completeOuters ++ connectSegments(partialOuters.sortWith(_.length > _.length)), completeInners ++ connectSegments(partialInners.sortWith(_.length > _.length)))) {
          case ((o: List[Line], i: List[Line]), u) =>
            if (o.exists(Polygon(_).contains(u))) {
              (o, i :+ u)
            } else {
              (o :+ u, i)
            }
        }

        // reclassify rings according to their topology (ignoring roles)
        val (classifiedOuters, classifiedInners) = (outers ++ inners).map(Polygon(_)).sortWith(_.area > _.area) match {
          case h :: t => t.foldLeft((List(h), List.empty[Polygon])) {
            case ((os, is), ring) =>
              ring match {
                // there's an outer ring that contains this one; this is an inner ring
                case _ if os.exists(or => or.contains(ring)) => (os, is :+ ring)
                case _ => (os :+ ring, is)
              }
          }
          case Nil => (List.empty[Polygon], List.empty[Polygon])
        }

        val (dissolvedOuters, addlInners) = dissolveRings(classifiedOuters)
        val (dissolvedInners, addlOuters) = dissolveRings(classifiedInners.map(_.exterior) ++ addlInners)

        val (polygons, _) = (dissolvedOuters ++ addlOuters)
          // sort by size (descending) to use rings as part of the largest available polygon
          .sortWith(Polygon(_).area > Polygon(_).area)
          // only use inners once if they're contained by multiple outer rings
          .foldLeft((List.empty[Polygon], dissolvedInners)) {
          case ((ps, is), (outer)) =>
            (ps :+ Polygon(outer, is.filter(inner => Polygon(outer).contains(inner))), is.filterNot(inner => Polygon(outer).contains(inner)))
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
