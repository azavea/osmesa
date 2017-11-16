package osmesa

import java.util.concurrent.TimeUnit

import geotrellis.vector.{Point, Line}
import geotrellis.util.Haversine
import org.openjdk.jmh.annotations._

// --- //

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
class MetresBench {

  var line0: Line = _
  var line1: Line = _

  @Setup
  def setup: Unit = {
    line0 = Line((0 to 9).map(n => Point(n, n)))
    line1 = Line((0 to 90).map(n => Point(n, n)))
  }

  def iterator(line: Line): Double = {
    val ps: List[Point] = line.points.toList
    val pairs: Iterator[(Point, Point)] = ps.iterator.zip(ps.tail.iterator)

    pairs.foldLeft(0d) { case (acc, (p,c)) => acc + Haversine(p.x, p.y, c.x, c.y) }
  }

  def manual(line: Line): Double = {
    val geom = line.jtsGeom

    (0 until (geom.getNumPoints - 1)).map { i =>
      val p = geom.getPointN(i)
      val c = geom.getPointN(i + 1)

      Haversine(p.getX, p.getY, c.getX, c.getY)
    } reduce (_ + _)
  }

  def whiley(line: Line): Double = {
    val geom = line.jtsGeom
    var i: Int = 0
    var r: Double = 0

    while (i < geom.getNumPoints - 1) {
      val p = geom.getPointN(i)
      val c = geom.getPointN(i + 1)

      r += Haversine(p.getX, p.getY, c.getX, c.getY)
      i += 1
    }

    r
  }

  def sliding(line: Line): Double = {
    val geom = line.jtsGeom

    line.points.sliding(2)
      .map(pair => Haversine(pair.head.x, pair.head.y, pair.last.x, pair.last.y))
      .foldLeft(0d) { _ + _ }
  }

  @Benchmark
  def iterator10: Double = iterator(line0)
  @Benchmark
  def iterator100: Double = iterator(line1)

  @Benchmark
  def manual10: Double = manual(line0)
  @Benchmark
  def manual100: Double = manual(line1)

  @Benchmark
  def while10: Double = whiley(line0)
  @Benchmark
  def while100: Double = whiley(line1)

  @Benchmark
  def sliding10: Double = sliding(line0)
  @Benchmark
  def sliding100: Double = sliding(line1)

}
