package osmesa.common.model

import java.math.BigDecimal
import java.sql.Timestamp

import org.joda.time.DateTime
import osmesa.common.ProcessOSM.{NodeType, RelationType, WayType}
import osmesa.common.model.Actions.Action

import scala.xml.Node

// TODO at some point user metadata (changeset, uid, user, timestamp?) should become options, as they may not be
// available
case class Element(_type: Byte,
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

object Element {
  implicit def stringToTimestamp(s: String): Timestamp =
    Timestamp.from(DateTime.parse(s).toDate.toInstant)

  def fromXML(node: Node, action: Action): Element = {
    val _type = node.label match {
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
    val nds = _type match {
      case WayType =>
        Some((node \ "nd").map(n => (n \@ "ref").toLong))
      case _ => None
    }
    val members = _type match {
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

    Element(_type,
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
