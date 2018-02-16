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

  def dlt(pairs: Seq[(Point, Point)], cx: Double, cy: Double): DoubleMatrix = {
    val m = new DoubleMatrix(pairs.length * 2, 9)

    pairs
      .flatMap({ case (a: Point, b: Point) => pairToRows(a, b, cx, cy, 1e-5, 1e-5) })
      .zipWithIndex
      .map({ case (c: DoubleMatrix, i: Int) => m.putRow(i, c) })

    val svd = Singular.fullSVD(m)
    val h = svd(2).getColumn(8).reshape(3,3).transpose()
    val h33 = h.get(2,2)

    h.div(h33)
  }

  def dlt(vs: Seq[Point], ws: Seq[Point]): DoubleMatrix = {
    val xmin = vs.map({ p: Point => p.x }).reduce(math.min(_,_))
    val xmax = vs.map({ p: Point => p.x }).reduce(math.max(_,_))
    val xmid = (xmax+xmin)/2.0
    val ymin = vs.map({ p: Point => p.y }).reduce(math.min(_,_))
    val ymax = vs.map({ p: Point => p.y }).reduce(math.max(_,_))
    val ymid = (ymax+ymin)/2.0
    val maxabsx = math.max(math.abs(xmin),math.abs(xmax))
    val maxabsy = math.max(math.abs(ymin),math.abs(ymax))

    val pairs = vs.zip(ws).drop(1)
    val m = new DoubleMatrix(pairs.length * 2, 9)

    pairs
      .flatMap({ case (a: Point, b: Point) => pairToRows(a,b,xmid,ymid,maxabsx,maxabsy) })
      .zipWithIndex
      .map({ case (c: DoubleMatrix, i: Int) => m.putRow(i, c) })

    val svd = Singular.fullSVD(m)
    val h = svd(2).getColumn(8).reshape(3,3).transpose()
    val h33 = h.get(2,2)

    h.div(h33)
  }

  def dlt(left: Geometry, right: Geometry): DoubleMatrix = {
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

    dlt(vs, ws)
  }

}
