package osmesa.bm

import geotrellis.vector._
import geotrellis.vector.io._

import com.vividsolutions.jts.algorithm.{Centroid, CGAlgorithms}
import com.vividsolutions.jts.geom.Coordinate


object VertexMatching {

  private def matcher(
    points1: Array[Point], points2: Array[Point], skips: Int,
    offsetx: Double, offsety: Double,
    best: Double = Double.MaxValue, current: Double = 0.0,
    list: List[(Point, Point)] = List.empty[(Point, Point)]
  ): (Double, List[(Point, Point)]) = {
    val end = (Double.MaxValue, List.empty[(Point, Point)])
    if (points1.isEmpty) {
      if (current < best) (current, list)
      else end
    }
    else if (points2.isEmpty) end
    else {
      val doSkip =
        if (skips > 0) matcher(
          points1.drop(1), points2, skips-1,
          offsetx, offsety,
          best, current, list
        )
        else end

      val dontSkip = {
        val (d, i) = argmin(points1.head, points2, offsetx, offsety)
        matcher(
          points1.drop(1), points2.drop(i+1), skips,
          offsetx, offsety,
          best, current+d, list ++ List((points1.head, points2(i)))
        )
      }
      if (doSkip._1 < dontSkip._1) doSkip
      else dontSkip
    }
  }

  private def argmin(
    p: Point, ps: Array[Point],
    offsetx: Double, offsety: Double
  ): (Double, Int) = {
    ps
      .map({ p2 =>
        val temp = Point(p2.x - offsetx, p2.y - offsety)
        temp.distance(p)
      })
      .zipWithIndex
      .reduce({ (pair1, pair2) =>
        if (pair1._1 <= pair2._1) pair1
        else pair2
      })
  }

  def apply(_p1: Polygon, _p2: Polygon) = {
    val (p1, p2) =
      if (_p1.vertices.length < _p2.vertices.length) (_p1, _p2)
      else (_p2, _p1)

    val (centroidx, centroidy) = {
      val centroid = Centroid.getCentroid(p1.jtsGeom)
      (centroid.x, centroid.y)
    }

    val (offsetx, offsety) = {
      val centroid1 = Centroid.getCentroid(p1.jtsGeom)
      val centroid2 = Centroid.getCentroid(p2.jtsGeom)
      (centroid2.x - centroidx, centroid2.y - centroidy)
    }

    val points1 = {
      val pts = p1.jtsGeom.getCoordinates
      if (CGAlgorithms.isCCW(pts)) pts
      else pts.reverse
    }.drop(1).map({ p => Point(p.x, p.y) })

    val points2 = {
      val points = {
        val pts = p2.jtsGeom.getCoordinates
        if (CGAlgorithms.isCCW(pts)) pts
        else pts.reverse
      }.drop(1).map({ p => Point(p.x, p.y) })
      val (_, i) = argmin(points1.head, points, offsetx, offsety)
      points.drop(i) ++ points.take(i)
    }

    val (_, pairs) = matcher(points1, points2, points1.length-4, offsetx, offsety)

    Homography.dlt(pairs, centroidx, centroidy)
  }

  def main(args: Array[String]): Unit = {
    val polygon1 =
      if (args(0).endsWith(".geojson"))
        scala.io.Source.fromFile(args(0)).mkString.parseGeoJson[Polygon]
      else
        args(0).parseGeoJson[Polygon]

    val polygon2 =
      if (args(1).endsWith(".geojson"))
        scala.io.Source.fromFile(args(1)).mkString.parseGeoJson[Polygon]
      else
        args(1).parseGeoJson[Polygon]

    println(apply(polygon1, polygon2))
  }

}
