package osmesa.functions.osm

import geotrellis.vector.{Line, Point, Polygon}
import geotrellis.vector.io._
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class WayAssembler extends UserDefinedAggregateFunction {
  override def inputSchema: StructType =
    StructType(
      StructField("isArea", BooleanType, nullable = false) ::
        StructField("idx", IntegerType, nullable = false) ::
        StructField("lon", DoubleType) ::
        StructField("lat", DoubleType) ::
        Nil)

  // sparse array of coordinates
  override def bufferSchema: StructType =
    StructType(
      StructField("coords", ArrayType(ArrayType(DoubleType)), nullable = false) ::
        StructField("isArea", BooleanType, nullable = false) ::
        Nil
    )

  override def dataType: DataType = BinaryType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array[Array[Double]]()
    buffer(1) = false
  }

  private def ensureSize[A](buffer: mutable.Buffer[A], size: Int)(implicit m: ClassTag[A]): mutable.Buffer[A] = {
    if (size >= buffer.length) {
      // increase the size of the buffer to fit new items
      buffer.insertAll(buffer.length, new Array[A](size - buffer.length + 1))
    }

    buffer
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val coords = ArrayBuffer(buffer.getAs[Seq[Seq[Double]]](0): _*)
    val isArea = input.getAs[Boolean](0)
    val idx = input.getAs[Int](1)
    val lon = Option(input.get(2)).map(_.asInstanceOf[Double]).getOrElse(Double.NaN)
    val lat = Option(input.get(3)).map(_.asInstanceOf[Double]).getOrElse(Double.NaN)

    ensureSize(coords, idx).update(idx, Seq(lon, lat))

    buffer(0) = coords
    buffer(1) = isArea
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val coords = ArrayBuffer(buffer1.getAs[Seq[Seq[Double]]](0): _*)
    val leftIsArea = buffer1.getAs[Boolean](1)

    val right = buffer2.getAs[Seq[Seq[Double]]](0)
    val rightIsArea = buffer2.getAs[Boolean](1)

    for ((c, idx) <- right.zipWithIndex) {
      if (Option(c).isDefined) {
        ensureSize(coords, idx).update(idx, c)
      }
    }

    buffer1(0) = coords
    buffer1(1) = leftIsArea || rightIsArea
  }

  override def evaluate(buffer: Row): Any = {
    {
      val isArea = buffer.getAs[Boolean](1)

      buffer.getAs[Seq[Seq[Double]]](0) match {
        // no coordinates provided
        case coords if coords.isEmpty => Some("LINESTRING EMPTY".parseWKT)
        // some of the coordinates are empty; this is invalid
        case coords if coords.exists(Option(_).isEmpty) => None
        // some of the coordinates are invalid
        case coords if coords.exists(_.exists(_.isNaN)) => None
        // 1 pair of coordinates provided
        case coords if coords.length == 1 =>
          Some(Point(coords.head.head, coords.head.last))
        case coords => {
          coords.map(xy => (xy.head, xy.last)) match {
            case pairs => Line(pairs)
          }
        } match {
          case ring if isArea && ring.vertexCount >= 4 && ring.isClosed =>
            Some(Polygon(ring))
          case line => Some(line)
        }
      }
    } match {
      case Some(g) if g.isValid => g.toWKB(4326)
      case _ => null
    }
  }
}
