package osmesa.bm

import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json.JsonFeatureCollection


object VertexProjection {

  def pointToPolygon(p: Point, right: Polygon) = {

    right.vertices
      .sliding(2)
      .map({ case Array(a, b) =>
        val px: Double = p.x - a.x
        val py: Double = p.y - a.y
        val vx: Double = b.x - a.x
        val vy: Double = b.y - a.y
        val absv: Double = math.sqrt(vx*vx + vy*vy)
        val t = px*vx/absv + py*vy/absv

        val c =
          if (t <= 0.0) a
          else if (t >= 1.0) b
          else Point(a.x*t + b.x*(1.0-t), a.y*t + b.y*(1.0-t))

        (p.distance(c), c)
      })
      .reduce({ (t1: (Double, Point), t2: (Double, Point)) =>
        if (t1._1 <= t2._1) t1
        else t2
      })._2
  }

  def polygonToPolygon(left: Polygon, right: Polygon, debug: Boolean = false) = {
    val xs1 = left.vertices
    val xs2 = xs1.map({ point => pointToPolygon(point, right) })
    if (debug) println(xs1.zip(xs2).map({ case (p1: Point, p2: Point) => p1.distance(p2) }).toList)
    Homography.dlt(xs1, xs2)
  }

  def geometryToGeometry(left: Geometry, right: Geometry, debug: Boolean = false) = {
    val polygon1 = left match {
      case p: Polygon => p
      case mp: MultiPolygon =>
        mp.polygons.reduce({ (p1, p2) => if (p1.vertices.length > p2.vertices.length) p1; else p2 })
    }
    val polygon2 = right match {
      case p: Polygon => p
      case mp: MultiPolygon =>
        mp.polygons.reduce({ (p1, p2) => if (p1.vertices.length > p2.vertices.length) p1; else p2 })
    }
    polygonToPolygon(polygon1, polygon2, debug)
  }

  def main(args: Array[String]): Unit = {
    val polygon1 =
      if (args(0).endsWith(".geojson"))
        scala.io.Source.fromFile(args(0)).mkString.parseGeoJson[Geometry]
      else
        args(0).parseGeoJson[Geometry]

    val polygon2 =
      if (args(1).endsWith(".geojson"))
        scala.io.Source.fromFile(args(1)).mkString.parseGeoJson[Geometry]
      else
        args(1).parseGeoJson[Geometry]

    println(geometryToGeometry(polygon1, polygon2, debug = true))
    println(geometryToGeometry(polygon2, polygon1, debug = true))
 }

}
