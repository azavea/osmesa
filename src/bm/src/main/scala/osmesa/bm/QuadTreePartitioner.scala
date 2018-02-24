package osmesa.bm

import geotrellis.vector._

import vectorpipe.osm._

import org.apache.spark.{Partitioner, HashPartitioner }
import org.apache.spark.rdd.RDD


class QuadTreePartitioner(divisionSet: Set[Int], partitions: Int) extends Partitioner {

  val maxDivisions = divisionSet.reduce(math.max)

  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  val hashPartitioner = new HashPartitioner(partitions)

  private def step(
    bits: Int,
    _xmin: Double, _ymin: Double,
    _xmax: Double, _ymax:Double
  ): (Double, Double, Double, Double) = {
    var xmin = _xmin
    var ymin = _ymin
    var xmax = _xmax
    var ymax = _ymax

    bits match {
      case 0 =>
        xmin = 2*xmin
        ymin = 2*ymin
        xmax = 2*xmax
        ymax = 2*ymax
      case 1 =>
        xmin = 2*(xmin - 0.5)
        ymin = 2*ymin
        xmax = 2*(xmax - 0.5)
        ymax = 2*ymax
      case 2 =>
        xmin = 2*xmin
        ymin = 2*(ymin - 0.5)
        xmax = 2*xmax
        ymax = 2*(ymax - 0.5)
      case 3 =>
        xmin = 2*(xmin - 0.5)
        ymin = 2*(ymin - 0.5)
        xmax = 2*(xmax - 0.5)
        ymax = 2*(ymax - 0.5)
    }

    (xmin, ymin, xmax, ymax)
  }

  private def getBits(xmin: Double, ymin: Double, xmax: Double, ymax:Double): Option[Int] = {
    val minBits = ((xmin > 0.5),(ymin > 0.5)) match {
      case (false, false) => 0
      case (true, false) => 1
      case (false, true) => 2
      case (true, true) => 3
    }
    val maxBits = ((xmax > 0.5),(ymax > 0.5)) match {
      case (false, false) => 0
      case (true, false) => 1
      case (false, true) => 2
      case (true, true) => 3
    }

    if (minBits == maxBits) Some(minBits); else None
  }

  private def getBox(g: Geometry): (Double, Double, Double, Double) = {
    val e: Extent = g.envelope
    ((e.xmin+180)/360, (e.ymin+90)/180, (e.xmax+180)/360, (e.ymax+90)/180)
  }

  def getAddress(g: Geometry): Long = {
    var box: (Double, Double, Double, Double) = getBox(g)
    var address: Long = 0
    var bits: Option[Int] = getBits(box._1, box._2, box._3, box._4)
    var division = 0

    while (bits != None && division <= maxDivisions) {
      if (divisionSet.contains(division))
        address = (address << 2) | bits.get
      box = step(bits.get, box._1, box._2, box._3, box._4)
      bits = getBits(box._1, box._2, box._3, box._4)
      division = division + 1
    }

    address
  }

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    key match {
      case f: OSMFeature =>
        (getAddress(f.geom) % partitions).toInt
      case g: Geometry =>
        (getAddress(g) % partitions).toInt
      case _ =>
        throw new Exception
    }
  }

  override def equals(other: Any): Boolean = false

  override def hashCode: Int = numPartitions
}
