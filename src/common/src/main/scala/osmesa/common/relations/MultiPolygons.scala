package osmesa.common.relations
import java.sql.Timestamp

import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory
import com.vividsolutions.jts.geom.{Geometry, LineString, Polygon, TopologyException}
import org.apache.log4j.Logger
import osmesa.common.ProcessOSM.WayType

object MultiPolygons {
  private lazy val logger = Logger.getLogger(getClass)
  val prepGeomFactory = new PreparedGeometryFactory

  def build(id: Long,
            version: Int,
            timestamp: Timestamp,
            types: Seq[Byte],
            roles: Seq[String],
            _geoms: Seq[Geometry]): Option[Geometry] = {
    if (types.zip(_geoms).exists { case (t, g) => t == WayType && Option(g).isEmpty }) {
      // bail early if null values are present where they should exist (members w/ type=way)
      logger.debug(s"Incomplete relation: $id @ $version ($timestamp)")
      None
    } else {
      val geomCount = _geoms.map(Option(_)).count(_.isDefined)

      logger.debug(s"$id @ $version ($timestamp) ${geomCount.formatted("%,d")} geoms")
      val geoms = _geoms.map {
        case geom: Polygon    => Some(geom.getExteriorRing)
        case geom: LineString => Some(geom)
        case _                => None
      }

      val vertexCount = geoms.filter(_.isDefined).map(_.get).map(_.getNumPoints).sum
      logger.warn(s"${vertexCount.formatted("%,d")} vertices (${geomCount
        .formatted("%,d")} geoms) from ${types.size} members in $id @ $version ($timestamp)")

      val members: Seq[(String, LineString)] = roles
        .zip(geoms)
        .filter(_._2.isDefined)
        .map(x => (x._1, x._2.get))

      val (complete, partial) =
        members.foldLeft((Vector.empty[Polygon], Vector.empty[LineString])) {
          case ((c, p), (role, line: LineString)) =>
            role match {
              case "outer" if line.isClosed && line.getNumPoints >= 4 =>
                (c :+ geometryFactory.createPolygon(line.getCoordinates), p)
              case "outer" =>
                (c, p :+ line)
              case "inner" if line.isClosed && line.getNumPoints >= 4 =>
                (c :+ geometryFactory.createPolygon(line.getCoordinates), p)
              case "inner" => (c, p :+ line)
              case "" if line.isClosed && line.getNumPoints >= 4 =>
                (c :+ geometryFactory.createPolygon(line.getCoordinates), p)
              case "" =>
                (c, p :+ line)
              case _ =>
                (c, p)
            }
        }

      try {
        val rings = complete ++ formRings(partial.sortWith(_.getNumPoints > _.getNumPoints))
        val preparedRings = rings.map(prepGeomFactory.create)

        // reclassify rings according to their topology (ignoring roles)
        val (classifiedOuters, classifiedInners) = rings.sortWith(_.getArea > _.getArea) match {
          case Seq(h, t @ _*) =>
            t.foldLeft((Array(h), Array.empty[Polygon])) {
              case ((os, is), ring) =>
                // check the number of containing elements
                preparedRings.count(r => r.getGeometry != ring && r.contains(ring)) % 2 match {
                  // if even, it's an outer ring
                  case 0 => (os :+ ring, is)
                  // if odd, it's an inner ring
                  case 1 => (os, is :+ ring)
                }
            }
          case rs if rs.isEmpty => (Array.empty[Polygon], Array.empty[Polygon])
        }

        val (dissolvedOuters, addlInners) =
          dissolveRings(classifiedOuters)
        val (dissolvedInners, addlOuters) =
          dissolveRings(
            classifiedInners
              .map(_.getExteriorRing.getCoordinates)
              .map(geometryFactory.createPolygon) ++ addlInners)

        val (polygons, _) =
          (dissolvedOuters ++ addlOuters)
          // sort by size (descending) to use rings as part of the largest available polygon
            .sortWith(_.getArea > _.getArea)
            // only use inners once if they're contained by multiple outer rings
            .foldLeft((Vector.empty[Polygon], dissolvedInners)) {
              case ((ps, is), outer) =>
                val preparedOuter = prepGeomFactory.create(outer)
                (ps :+ geometryFactory.createPolygon(
                   geometryFactory.createLinearRing(outer.getExteriorRing.getCoordinates),
                   is.filter(inner => preparedOuter.contains(inner))
                     .map({ x => geometryFactory.createLinearRing(x.getExteriorRing.getCoordinates)
                     })
                     .toArray
                 ),
                 is.filterNot(inner => preparedOuter.contains(inner)))
            }

        polygons match {
          case v @ Vector(p: Polygon) if v.length == 1 => Some(p)
          case ps                                      => Some(geometryFactory.createMultiPolygon(ps.toArray))
        }
      } catch {
        case e @ (_: AssemblyException | _: IllegalArgumentException | _: TopologyException) =>
          logger.warn(
            s"Could not reconstruct relation $id @ $version ($timestamp): ${e.getMessage}")
          None
        case e: Throwable =>
          logger.warn(s"Could not reconstruct relation $id @ $version ($timestamp): $e")
          e.getStackTrace.foreach(logger.warn)
          None
      }
    }
  }
}
