package osmesa.common

import com.vividsolutions.jts.geom._
import osmesa.common.relations.utils.{
  PartialCoordinateSequence,
  ReversedCoordinateSequence,
  VirtualCoordinateSequence,
  isEqual
}

import scala.annotation.tailrec
import scala.collection.GenTraversable

package object relations {

  // join segments together
  @tailrec
  def connectSegments(segments: GenTraversable[VirtualCoordinateSequence],
                      lines: Seq[CoordinateSequence] = Vector.empty[CoordinateSequence])
    : GenTraversable[CoordinateSequence] = {
    segments match {
      case Nil =>
        lines
      case Seq(h, t @ _*) =>
        val x = h.getX(h.size - 1)
        val y = h.getY(h.size - 1)

        t.find(line => x == line.getX(0) && y == line.getY(0)) match {
          case Some(next) =>
            connectSegments(h.append(new PartialCoordinateSequence(next, 1)) +: t.filterNot(line =>
                              isEqual(line, next)),
                            lines)
          case None =>
            t.find(line => x == line.getX(line.size - 1) && y == line.getY(line.size - 1)) match {
              case Some(next) =>
                connectSegments(h.append(
                                  new PartialCoordinateSequence(
                                    new ReversedCoordinateSequence(next),
                                    1)) +: t.filterNot(line => isEqual(line, next)),
                                lines)
              case None => connectSegments(t, lines :+ h)
            }
        }
    }
  }

  def connectSegments(segments: GenTraversable[Geometry])(
      implicit geometryFactory: GeometryFactory): GenTraversable[LineString] =
    connectSegments(
      segments
        .flatMap {
          case geom: LineString => Some(geom.getCoordinateSequence)
          case _                => None
        }
        .map(s => new VirtualCoordinateSequence(Seq(s)))
    ).map(geometryFactory.createLineString)

  // since GeoTrellis's GeometryFactory is unavailable
  implicit val geometryFactory: GeometryFactory = new GeometryFactory()

  // join segments together into rings
  @tailrec
  def formRings(segments: GenTraversable[VirtualCoordinateSequence],
                        rings: Seq[CoordinateSequence] = Vector.empty[CoordinateSequence])
    : GenTraversable[CoordinateSequence] = {
    segments match {
      case Nil =>
        rings
      case Seq(h, t @ _*) if h.getX(0) == h.getX(h.size - 1) && h.getY(0) == h.getY(h.size - 1) =>
        formRings(t, rings :+ h)
      case Seq(h, t @ _*) =>
        val x = h.getX(h.size - 1)
        val y = h.getY(h.size - 1)

        formRings(
          t.find(line => x == line.getX(0) && y == line.getY(0)) match {
            case Some(next) =>
              h.append(new PartialCoordinateSequence(next, 1)) +: t.filterNot(line =>
                isEqual(line, next))
            case None =>
              t.find(line => x == line.getX(line.size - 1) && y == line.getY(line.size - 1)) match {
                case Some(next) =>
                  h.append(new PartialCoordinateSequence(new ReversedCoordinateSequence(next), 1)) +: t
                    .filterNot(line => isEqual(line, next))
                case None => throw new AssemblyException("Unable to connect segments.")
              }
          },
          rings
        )
    }
  }

  def formRings(segments: GenTraversable[LineString])(
      implicit geometryFactory: GeometryFactory): GenTraversable[Polygon] =
    formRings(segments.map(_.getCoordinateSequence).map(s => new VirtualCoordinateSequence(Seq(s))))
      .map(geometryFactory.createPolygon)

  def dissolveRings(rings: Array[Polygon]): (Seq[Polygon], Seq[Polygon]) = {
    Option(geometryFactory.createGeometryCollection(rings.asInstanceOf[Array[Geometry]]).union) match {
      case Some(mp) =>
        val polygons = for (i <- 0 until mp.getNumGeometries) yield {
          mp.getGeometryN(i).asInstanceOf[Polygon]
        }

        (polygons.map(_.getExteriorRing.getCoordinates).map(geometryFactory.createPolygon),
         polygons.flatMap(getInteriorRings).map(geometryFactory.createPolygon))
      case None =>
        (Vector.empty[Polygon], Vector.empty[Polygon])
    }
  }

  def getInteriorRings(p: Polygon): Seq[LinearRing] =
    for (i <- 0 until p.getNumInteriorRing)
      yield geometryFactory.createLinearRing(p.getInteriorRingN(i).getCoordinates)

  class AssemblyException(msg: String) extends Exception(msg)
}
