package osmesa.bm

import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json.JsonFeatureCollection

import org.locationtech.jts.algorithm.Centroid


object VertexProjection {

  private def pointToPolygon(p: Point, offsetx: Double, offsety: Double, right: Polygon): Point = {

    val point =
      right.vertices
        .map({ p => Point(p.x - offsetx, p.y - offsety) })
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

    Point(point.x + offsetx, point.y + offsety)
  }

  private def polygonToPolygon(left: Polygon, right: Polygon, relative: Boolean) = {
    val (centroidx, centroidy) = {
      val centroid = Centroid.getCentroid(left.jtsGeom)
      (centroid.x, centroid.y)
    }

    val (offsetx: Double, offsety: Double) = {
      if (relative) {
        val centroid = Centroid.getCentroid(right.jtsGeom)
        (centroid.x - centroidx, centroid.y - centroidy)
      }
      else (0.0, 0.0)
    }

    val xs1 = left.vertices
    val xs2 = xs1.map({ point => pointToPolygon(point, offsetx, offsety, right) })

    Homography.dlt(xs1.zip(xs2), centroidx, centroidy)
  }

  private def geometryToGeometry(left: Geometry, right: Geometry, relative: Boolean) = {
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

    polygonToPolygon(polygon1, polygon2, relative)
  }

  def score(p1: Polygon, p2: Polygon): Double = {
    val h1 = polygonToPolygon(p1, p2, false).toArray
    val Δ1 = math.abs(h1(0)-1.0) + math.abs(h1(1)) + math.abs(h1(2)) + math.abs(h1(3)) + math.abs(h1(4)-1.0) + math.abs(h1(5))

    val h2 = polygonToPolygon(p2, p1, false).toArray
    val Δ2 = math.abs(h2(0)-1.0) + math.abs(h2(1)) + math.abs(h2(2)) + math.abs(h2(3)) + math.abs(h2(4)-1.0) + math.abs(h2(5))

    val h3 = polygonToPolygon(p1, p2, true).toArray
    val Δ3 = math.abs(h3(0)-1.0) + math.abs(h3(1)) + math.abs(h3(2)) + math.abs(h3(3)) + math.abs(h3(4)-1.0) + math.abs(h3(5))

    val h4 = polygonToPolygon(p2, p1, true).toArray
    val Δ4 = math.abs(h4(0)-1.0) + math.abs(h4(1)) + math.abs(h4(2)) + math.abs(h4(3)) + math.abs(h4(4)-1.0) + math.abs(h4(5))

    math.min(Δ1, math.min(Δ2, math.min(Δ3, Δ4)))
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

    println(geometryToGeometry(polygon1, polygon2, false))
    println(geometryToGeometry(polygon2, polygon1, false))
    println(geometryToGeometry(polygon1, polygon2, true))
    println(geometryToGeometry(polygon2, polygon1, true))
  }

}
