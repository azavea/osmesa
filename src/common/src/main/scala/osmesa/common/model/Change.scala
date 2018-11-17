package osmesa.common.model

import java.math.BigDecimal
import java.sql.Timestamp

import org.apache.spark.sql.types._
import org.joda.time.DateTime
import osmesa.common.ProcessOSM.{NodeType, RelationType, WayType}
import osmesa.common.model.Actions.Action

import scala.xml.Node

// TODO at some point user metadata (changeset, uid, user, timestamp?) should become options, as they may not be
// available
case class Change(sequence: Int,
                  `type`: Byte,
                  id: Long,
                  tags: Map[String, String],
                  lat: Option[BigDecimal],
                  lon: Option[BigDecimal],
                  nds: Option[Seq[Long]],
                  members: Option[Seq[Member]],
                  changeset: Long,
                  timestamp: Timestamp,
                  uid: Long,
                  user: String,
                  version: Int,
                  visible: Boolean)

object Change {
  val Schema = StructType(
    StructField("sequence", IntegerType) ::
      StructField("type", ByteType, nullable = false) ::
      StructField("id", LongType, nullable = false) ::
      StructField(
        "tags",
        MapType(StringType, StringType, valueContainsNull = false),
        nullable = false
      ) ::
      StructField("lat", DataTypes.createDecimalType(9, 7), nullable = true) ::
      StructField("lon", DataTypes.createDecimalType(10, 7), nullable = true) ::
      StructField("nds", DataTypes.createArrayType(LongType), nullable = true) ::
      StructField(
        "members",
        DataTypes.createArrayType(
          StructType(
            StructField("type", ByteType, nullable = false) ::
              StructField("ref", LongType, nullable = false) ::
              StructField("role", StringType, nullable = false) ::
              Nil
          )
        ),
        nullable = true
      ) ::
      StructField("changeset", LongType, nullable = false) ::
      StructField("timestamp", TimestampType, nullable = false) ::
      StructField("uid", LongType, nullable = false) ::
      StructField("user", StringType, nullable = false) ::
      StructField("version", IntegerType, nullable = false) ::
      StructField("visible", BooleanType, nullable = false) ::
      Nil
  )

  implicit def stringToTimestamp(s: String): Timestamp =
    Timestamp.from(DateTime.parse(s).toDate.toInstant)

  def fromXML(node: Node, action: Action, sequence: Int): Change = {
    val `type` = node.label match {
      case "node"     => NodeType
      case "way"      => WayType
      case "relation" => RelationType
    }
    val id = (node \@ "id").toLong
    val tags =
      (node \ "tag").map(tag => (tag \@ "k", tag \@ "v")).toMap
    val lat = node \@ "lat" match {
      case "" => None
      case v  => Some(new BigDecimal(v))
    }
    val lon = node \@ "lon" match {
      case "" => None
      case v  => Some(new BigDecimal(v))
    }
    val nds = `type` match {
      case WayType =>
        Some((node \ "nd").map(n => (n \@ "ref").toLong))
      case _ => None
    }
    val members = `type` match {
      case RelationType =>
        Some((node \ "member").map(Member.fromXML))
      case _ => None
    }
    val changeset = (node \@ "changeset").toLong
    val timestamp = node \@ "timestamp"
    val uid = (node \@ "uid").toLong
    val user = node \@ "user"
    val version = (node \@ "version").toInt
    val visible = action match {
      case Actions.Create | Actions.Modify => true
      case Actions.Delete                  => false
    }

    Change(sequence,
      `type`,
      id,
      tags,
      lat,
      lon,
      nds,
      members,
      changeset,
      timestamp,
      uid,
      user,
      version,
      visible)
  }
}
