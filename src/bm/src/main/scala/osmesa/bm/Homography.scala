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
      .foreach({ case (c: DoubleMatrix, i: Int) => m.putRow(i, c) })

    val svd = Singular.fullSVD(m)
    val h = svd(2).getColumn(8).reshape(3,3).transpose()
    val h33 = h.get(2,2)

    h.div(h33)
  }

}
