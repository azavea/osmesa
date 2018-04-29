package osmesa.functions.osm

import java.sql.Timestamp

import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import osmesa.ProcessOSM

class RelationAssembler extends UserDefinedAggregateFunction {
  val InputSchema: StructType =
    StructType(
      StructField("id", DataTypes.LongType, nullable = false) ::
        StructField("version", DataTypes.IntegerType, nullable = false) ::
        StructField("updated", DataTypes.TimestampType, nullable = false) ::
        StructField("type", DataTypes.StringType) ::
        StructField("role", DataTypes.StringType) ::
        StructField("geom", DataTypes.BinaryType) ::
        Nil)

  private val BufferSchema: StructType =
    StructType(
      StructField("id", DataTypes.LongType) ::
        StructField("version", DataTypes.IntegerType) ::
        StructField("updated", DataTypes.TimestampType) ::
        StructField("type", ArrayType(DataTypes.ByteType)) ::
        StructField("role", ArrayType(DataTypes.StringType)) ::
        StructField("geom", ArrayType(DataTypes.BinaryType)) ::
        Nil
    )

  lazy private val logger = Logger.getLogger(getClass)

  override def inputSchema: StructType = InputSchema

  override def bufferSchema: StructType = BufferSchema

  override def dataType: DataType = BinaryType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, null)
    buffer.update(1, null)
    buffer.update(2, null)
    buffer.update(3, Seq[Byte]())
    buffer.update(4, Seq[String]())
    buffer.update(5, Seq[Array[Byte]]())
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val id = input.getAs[Long](0)
    val version = input.getAs[Int](1)
    val updated = input.getAs[Timestamp](2)
    val memberType = input.getAs[String](3) match {
      case "node" => ProcessOSM.NodeType
      case "way" => ProcessOSM.WayType
      case "relation" => ProcessOSM.RelationType
      case _ => null
    }
    val memberRole = input.getAs[String](4)

    if (Option(memberRole).isEmpty || ProcessOSM.MultiPolygonRoles.contains(memberRole)) {
      val geom = input.getAs[Array[Byte]](5)

      val _id = buffer.getAs[Long](0)
      val _version = buffer.getAs[Int](1)
      val _updated = buffer.getAs[Timestamp](2)
      val types = buffer.getAs[Seq[Byte]](3)
      val roles = buffer.getAs[Seq[String]](4)
      val geoms = buffer.getAs[Seq[Array[Byte]]](5)

      // always the same
      if (_id != id) {
        buffer.update(0, id)
      }

      if (_version != version) {
        buffer.update(1, version)
      }

      if (_updated != updated) {
        buffer.update(2, updated)
      }

      buffer.update(3, types :+ memberType)
      buffer.update(4, roles :+ memberRole)
      buffer.update(5, geoms :+ geom)
    } else {
      logger.debug(s"Dropping member with role $memberRole")
    }
  }

  override def merge(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val leftId = Option(buffer(0)).map(_.asInstanceOf[Long])
    val leftVersion = Option(buffer(1)).map(_.asInstanceOf[Int])
    val leftUpdated = buffer.getAs[Timestamp](2)
    val leftTypes = buffer.getAs[Seq[Byte]](3)
    val leftRoles = buffer.getAs[Seq[String]](4)
    val leftGeoms = buffer.getAs[Seq[Array[Byte]]](5)

    val rightId = Option(input(0)).map(_.asInstanceOf[Long])
    val rightVersion = Option(input(1)).map(_.asInstanceOf[Int])
    val rightUpdated = input.getAs[Timestamp](2)
    val rightTypes = input.getAs[Seq[Byte]](3)
    val rightRoles = input.getAs[Seq[String]](4)
    val rightGeoms = input.getAs[Seq[Array[Byte]]](5)

    val id = leftId.orElse(rightId)
    val version = leftVersion.orElse(rightVersion)
    val updated = Option(leftUpdated).getOrElse(rightUpdated)

    if (leftId != id) {
      buffer.update(0, id.orNull)
    }

    if (leftVersion != version) {
      buffer.update(1, version.orNull)
    }

    if (leftUpdated != updated) {
      buffer.update(2, updated)
    }

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

    buildMultiPolygon(id, version, updated, types, roles, geoms).orNull
  }
}
