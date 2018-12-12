package osmesa.common.model

import java.sql.Timestamp

import org.apache.spark.sql.types._
import org.joda.time.DateTime
import osmesa.common.model.Actions.Action

import scala.xml.Node

// TODO at some point user metadata (changeset, uid, user, timestamp?) should become options, as they may not be
// available
case class Change(id: Long,
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
                  visible: Boolean,
                  sequence: Int)

object Change {
  val Schema = StructType(
    StructField("id", LongType) ::
      StructField("type", StringType) ::
      StructField(
      "tags",
      MapType(StringType, StringType, valueContainsNull = true)
    ) ::
      StructField("lat", DoubleType) ::
      StructField("lon", DoubleType) ::
      StructField(
      "nds",
      DataTypes.createArrayType(StructType(StructField("ref", LongType) :: Nil))
    ) ::
      StructField(
      "members",
      DataTypes.createArrayType(
        StructType(
          StructField("type", StringType) ::
            StructField("ref", LongType) ::
            StructField("role", StringType) ::
            Nil
        )
      )
    ) ::
      StructField("changeset", LongType) ::
      StructField("timestamp", TimestampType) ::
      StructField("uid", LongType) ::
      StructField("user", StringType) ::
      StructField("version", LongType) ::
      StructField("visible", BooleanType) ::
      StructField("sequence", IntegerType) ::
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

    Change(id,
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
           visible,
           sequence)
  }
}
