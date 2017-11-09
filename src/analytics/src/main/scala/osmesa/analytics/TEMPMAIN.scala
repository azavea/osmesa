package temp

import geotrellis.vector._
import com.madhukaraphatak.sizeof.SizeEstimator
import com.vividsolutions.jts.{geom => jts}

object Main {
  def sbytes(bytes: Long, si: Boolean = false): String = {
    val unit = if(si) { 1000 } else { 1024 }
    if (bytes < unit) {
      bytes + " B"
    } else {
      val exp = (math.log(bytes) / math.log(unit)).toInt
      val pre = (if(si) { "kMGTPE" } else { "KMGTPE" }).charAt(exp-1) + (if(si) { "" } else { "i" })
      f"${bytes / math.pow(unit, exp)}%.1f ${pre}B"
    }
  }

  def main(args: Array[String]): Unit = {
    println(s"A million points: ${sbytes(SizeEstimator.estimate(Point(2,2))* 1000000)}")
    println(s"A million JTS points: ${sbytes(SizeEstimator.estimate(Point(2,2).jtsGeom)* 1000000)}")
    println(s"A million coords points: ${sbytes(SizeEstimator.estimate(new jts.Coordinate(2,2))* 1000000)}")
    println(s"A million double tuples: ${sbytes(SizeEstimator.estimate((2.0, 1.0))* 1000000)}")
    println(sbytes(SizeEstimator.estimate(Point(2,2).jtsGeom)))
  }
}
