package osmesa.bm

import geotrellis.vector._

import vectorpipe.osm._

import org.jblas.{DoubleMatrix, Eigen, Singular}


object Homography {

  private def pairToRows(
    a: Point, b: Point,
    xbar: Double, ybar: Double,
    maxabsx: Double, maxabsy: Double
  ) = {
    val x: Double = (a.x - xbar) / maxabsx
    val y: Double = (a.y - ybar) / maxabsy
    val u: Double = (b.x - xbar) / maxabsx
    val v: Double = (b.y - ybar) / maxabsy

    Array(
      (new DoubleMatrix(Array(-x, -y, -1.0, 0.0, 0.0, 0.0, u*x, u*y, u))).transpose,
      (new DoubleMatrix(Array(0.0, 0.0, 0.0, -x, -y, -1.0, v*x, v*y, v))).transpose
    )
  }

  def dlt(left: Geometry, right: Geometry) = {
    val vs = left match {
      case p: Polygon => p.vertices
      case mp: MultiPolygon => mp.vertices
      case _ => throw new Exception
    }
    val ws = right match {
      case p: Polygon => p.vertices
      case mp: MultiPolygon => mp.vertices
      case _ => throw new Exception
    }
    val xbar = vs.map({ p: Point => p.x }).sum / vs.length
    val ybar = vs.map({ p: Point => p.y }).sum / vs.length
    val maxabsx = vs.map({ p: Point => math.abs(p.x - xbar) }).reduce(math.max(_,_))
    val maxabsy = vs.map({ p: Point => math.abs(p.y - ybar) }).reduce(math.max(_,_))

    val pairs = vs.zip(ws).drop(1)
    val m = new DoubleMatrix(pairs.length * 2, 9)

    pairs
      .flatMap({ case (a: Point, b: Point) => pairToRows(a,b,xbar,ybar,maxabsx,maxabsy) })
      .zipWithIndex
      .map({ case (c: DoubleMatrix, i: Int) => m.putRow(i, c) })

    val svd = Singular.fullSVD(m)

    svd(2).getColumn(8).reshape(3,3)
  }

  def kappa(h: DoubleMatrix): Double = {
    val spectrum = Eigen.eigenvalues(h).getReal.toArray
    spectrum(0) / spectrum(2)
  }

  def kappa(left: Geometry, right: Geometry): Double = {
    kappa(dlt(left, right))
  }

}
