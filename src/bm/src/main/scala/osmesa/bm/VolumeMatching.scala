package osmesa.bm

import geotrellis.vector._
import geotrellis.vector.io._


object VolumeMatching {

  def data(p1: Polygon, p2: Polygon): (Double, Double) = {
    val a1 = p1.jtsGeom.getArea
    val a2 = p2.jtsGeom.getArea
    val a3 = p1.jtsGeom.intersection(p2.jtsGeom).getArea
    (a3/a1, a3/a2)
  }

  def min(p1: Polygon, p2: Polygon): Double = {
    val (a1, a2) = data(p1, p2)
    math.min(a1, a2)
  }

  def max(p1: Polygon, p2: Polygon): Double = {
    val (a1, a2) = data(p1, p2)
    math.max(a1, a2)
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

    println(polygon1 == polygon2)
    println(data(polygon1, polygon2))
  }

}
