package osmesa.common.model

import java.sql.Timestamp

import org.apache.spark.sql.types._
import org.joda.time.DateTime
import osmesa.common.model.Actions.Action

import scala.xml.Node

// TODO at some point user metadata (changeset, uid, user, timestamp?) should become options, as they may not be
// available
case class Change(sequence: Int,
                  id: Long,
                  `type`: String,
                  tags: Map[String, String],
                  lat: Option[Double],
                  lon: Option[Double],
                  nds: Option[Seq[Nd]],
                  members: Option[Seq[Member]],
                  changeset: Long,
                  timestamp: Timestamp,
                  uid: Long,
                  user: String,
                  version: Long,
                  visible: Boolean)

object Change {
  val Schema = StructType(
    StructField("sequence", IntegerType) ::
      StructField("id", LongType, nullable = true) ::
      StructField("type", StringType, nullable = true) ::
      StructField(
      "tags",
      MapType(StringType, StringType, valueContainsNull = true),
      nullable = true
    ) ::
      StructField("lat", DoubleType, nullable = true) ::
      StructField("lon", DoubleType, nullable = true) ::
      StructField(
      "nds",
      DataTypes.createArrayType(StructType(StructField("ref", LongType, nullable = true) :: Nil)),
      nullable = true) ::
      StructField(
      "members",
      DataTypes.createArrayType(
        StructType(
          StructField("type", StringType, nullable = true) ::
            StructField("ref", LongType, nullable = true) ::
            StructField("role", StringType, nullable = true) ::
            Nil
        )
      ),
      nullable = true
    ) ::
      StructField("changeset", LongType, nullable = true) ::
      StructField("timestamp", TimestampType, nullable = true) ::
      StructField("uid", LongType, nullable = true) ::
      StructField("user", StringType, nullable = true) ::
      StructField("version", LongType, nullable = true) ::
      StructField("visible", BooleanType, nullable = true) ::
      Nil
  )

  implicit def stringToTimestamp(s: String): Timestamp =
    Timestamp.from(DateTime.parse(s).toDate.toInstant)

  def fromXML(node: Node, action: Action, sequence: Int): Change = {
    val `type` = node.label
    val id = (node \@ "id").toLong
    val tags =
      (node \ "tag").map(tag => (tag \@ "k", tag \@ "v")).toMap
    val lat = node \@ "lat" match {
      case "" => None
      case v  => Some(v.toDouble)
    }
    val lon = node \@ "lon" match {
      case "" => None
      case v  => Some(v.toDouble)
    }
    val nds = `type` match {
      case "way" =>
      Some((node \ "nd").map(Nd.fromXML))
      case _ => None
    }
    val members = `type` match {
      case "relation" =>
        Some((node \ "member").map(Member.fromXML))
      case _ => None
    }
    val changeset = (node \@ "changeset").toLong
    val timestamp = node \@ "timestamp"
    val uid = (node \@ "uid").toLong
    val user = node \@ "user"
    val version = (node \@ "version").toLong
    val visible = action match {
      case Actions.Create | Actions.Modify => true
      case Actions.Delete                  => false
    }

    Change(sequence,
      id,
      `type`,
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
