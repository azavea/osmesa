package osmesa.functions.osm

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import osmesa.ProcessOSM

class RelationAssembler extends UserDefinedAggregateFunction {
  override def inputSchema: StructType =
    StructType(
      StructField("id", LongType, nullable = false) ::
        StructField("version", IntegerType, nullable = false) ::
        StructField("updated", TimestampType, nullable = false) ::
        StructField("type", StringType) ::
        StructField("role", StringType) ::
        StructField("geom", BinaryType) ::
        Nil)

  // sparse array of coordinates
  override def bufferSchema: StructType =
    StructType(
      StructField("id", LongType) ::
        StructField("version", IntegerType) ::
        StructField("updated", TimestampType) ::
        StructField("type", ArrayType(ByteType)) ::
        StructField("role", ArrayType(StringType)) ::
        StructField("geom", ArrayType(BinaryType)) ::
        Nil
    )

  override def dataType: DataType = BinaryType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(3, Seq[Byte]())
    buffer.update(4, Seq[String]())
    buffer.update(5, Seq[Array[Byte]]())
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val id = input.getAs[Long](0)
    val version = input.getAs[Int](1)
    val updated = input.getAs[Timestamp](2)
    val memberType = input.getAs[String](3) match {
      case "node" => ProcessOSM.NODE_TYPE
      case "way" => ProcessOSM.WAY_TYPE
      case "relation" => ProcessOSM.RELATION_TYPE
      case _ => null
    }
    val memberRole = input.getAs[String](4)
    val geom = input.getAs[Array[Byte]](5)

    val types = buffer.getAs[Seq[Byte]](3)
    val roles = buffer.getAs[Seq[String]](4)
    val geoms = buffer.getAs[Seq[Array[Byte]]](5)

    // always the same
    buffer.update(0, id)
    buffer.update(1, version)
    buffer.update(2, updated)

    buffer.update(3, types :+ memberType)
    buffer.update(4, roles :+ memberRole)
    buffer.update(5, geoms :+ geom)
  }

  override def merge(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val leftId = buffer.getAs[Long](0)
    val leftVersion = buffer.getAs[Int](1)
    val leftUpdated = buffer.getAs[Timestamp](2)
    val leftTypes = buffer.getAs[Seq[Byte]](3)
    val leftRoles = buffer.getAs[Seq[String]](4)
    val leftGeoms = buffer.getAs[Seq[Array[Byte]]](5)

    val rightId = input.getAs[Long](0)
    val rightVersion = input.getAs[Int](1)
    val rightUpdated = input.getAs[Timestamp](2)
    val rightTypes = input.getAs[Seq[Byte]](3)
    val rightRoles = input.getAs[Seq[String]](4)
    val rightGeoms = input.getAs[Seq[Array[Byte]]](5)

    buffer.update(0, Option(leftId).getOrElse(rightId))
    buffer.update(1, Option(leftVersion).getOrElse(rightVersion))
    buffer.update(2, Option(leftUpdated).getOrElse(rightUpdated))
    buffer.update(3, leftTypes ++ rightTypes)
    buffer.update(4, leftRoles ++ rightRoles)
    buffer.update(5, leftGeoms ++ rightGeoms)
  }

  override def evaluate(buffer: Row): Any = {
    val id = buffer.getAs[Long](0)
    val version = buffer.getAs[Int](1)
    val updated = buffer.getAs[Timestamp](2)
    val types = buffer.getAs[Seq[Byte]](3)
    val roles = buffer.getAs[Seq[String]](4)
    val geoms = buffer.getAs[Seq[Array[Byte]]](5)

    buildMultiPolygon(id, version, updated, types, roles, geoms)
  }
}
