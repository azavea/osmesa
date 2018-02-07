package osmesa.bm

import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json.JsonFeatureCollection


object VertexMatching {

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

  def polygonToPolygon(left: Polygon, right: Polygon) = {
    val xs1 = left.vertices
    val xs2 = xs1.map({ point => pointToPolygon(point, right) })
    Homography.dlt(xs1, xs2)
  }

  def main(args: Array[String]): Unit = {
    val polygon1 = scala.io.Source.fromFile(args(0)).mkString.parseGeoJson[Geometry] match {
      case p: Polygon => p
      case mp: MultiPolygon =>
        mp.polygons.reduce({ (p1, p2) => if (p1.vertices.length > p2.vertices.length) p1; else p2 })
    }
    val polygon2 = scala.io.Source.fromFile(args(1)).mkString.parseGeoJson[Geometry] match {
      case p: Polygon => p
      case mp: MultiPolygon =>
        mp.polygons.reduce({ (p1, p2) => if (p1.vertices.length > p2.vertices.length) p1; else p2 })
    }

    println(polygonToPolygon(polygon1, polygon2))
    println(polygonToPolygon(polygon2, polygon1))
 }

}
