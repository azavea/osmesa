package osmesa.common.model
import java.sql.Timestamp

import com.vividsolutions.jts.{geom => jts}
import geotrellis.vector.io._
import geotrellis.vector.{Feature, Geometry => GTGeometry}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, Encoder => SparkEncoder}
import org.apache.spark.sql.jts._
import osmesa.common.ProcessOSM

case class AugmentedDiff(sequence: Int,
                         `type`: Byte,
                         id: Long,
                         prevGeom: Option[jts.Geometry],
                         geom: jts.Geometry,
                         prevTags: Option[Map[String, String]],
                         tags: Map[String, String],
                         prevChangeset: Option[Long],
                         changeset: Long,
                         prevUid: Option[Long],
                         uid: Long,
                         prevUser: Option[String],
                         user: String,
                         prevUpdated: Option[Timestamp],
                         updated: Timestamp,
                         prevVisible: Option[Boolean],
                         visible: Boolean,
                         prevVersion: Option[Int],
                         version: Int,
                         minorVersion: Boolean)

object AugmentedDiff {
  val Schema: StructType = StructType(
    StructField("sequence", IntegerType) ::
      StructField("type", ByteType, nullable = false) ::
      StructField("id", LongType, nullable = false) ::
      StructField("prevGeom", GeometryUDT, nullable = true) ::
      StructField("geom", GeometryUDT, nullable = true) ::
      StructField(
      "prevTags",
      MapType(StringType, StringType, valueContainsNull = false),
      nullable = true
    ) ::
      StructField(
      "tags",
      MapType(StringType, StringType, valueContainsNull = false),
      nullable = false
    ) ::
      StructField("prevChangeset", LongType, nullable = true) ::
      StructField("changeset", LongType, nullable = false) ::
      StructField("prevUid", LongType, nullable = true) ::
      StructField("uid", LongType, nullable = false) ::
      StructField("prevUser", StringType, nullable = true) ::
      StructField("user", StringType, nullable = false) ::
      StructField("prevUpdated", TimestampType, nullable = true) ::
      StructField("updated", TimestampType, nullable = false) ::
      StructField("prevVisible", BooleanType, nullable = true) ::
      StructField("visible", BooleanType, nullable = false) ::
      StructField("prevVersion", IntegerType, nullable = true) ::
      StructField("version", IntegerType, nullable = false) ::
      StructField("minorVersion", BooleanType, nullable = false) ::
      Nil
  )
  val Encoder: SparkEncoder[Row] = RowEncoder(Schema)

  def apply(sequence: Int,
            prev: Option[Feature[GTGeometry, ElementWithSequence]],
            curr: Feature[GTGeometry, ElementWithSequence]): AugmentedDiff = {
    val `type` = curr.data.`type` match {
      case "node"     => ProcessOSM.NodeType
      case "way"      => ProcessOSM.WayType
      case "relation" => ProcessOSM.RelationType
    }

    val minorVersion = prev.map(_.data.version).getOrElse(Int.MinValue) == curr.data.version

    AugmentedDiff(
      sequence,
      `type`,
      curr.data.id,
      prev.map(_.geom.jtsGeom),
      curr.geom.jtsGeom,
      prev.map(_.data.tags),
      curr.data.tags,
      prev.map(_.data.changeset),
      curr.data.changeset,
      prev.map(_.data.uid),
      curr.data.uid,
      prev.map(_.data.user),
      curr.data.user,
      prev.map(_.data.timestamp),
      curr.data.timestamp,
      prev.map(_.data.visible.getOrElse(true)),
      curr.data.visible.getOrElse(true),
      prev.map(_.data.version),
      curr.data.version,
      minorVersion
    )
  }
}
