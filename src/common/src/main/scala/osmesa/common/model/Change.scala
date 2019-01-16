package osmesa.common.model

import java.sql.Timestamp

import org.joda.time.DateTime
import osmesa.common.model.Actions.Action

import org.xml.sax
import org.xml.sax.helpers.DefaultHandler
import scala.collection.mutable.{Queue, Stack}
import scala.xml.{Elem, Node, Null, Text, Attribute}

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
  def empty: Change = Change(-1, "", Map.empty, None, None, None, None, -1, "1970-01-01 00:00:00".asInstanceOf[Timestamp], -1, "", -1, false, -1)

  implicit def stringToTimestamp(s: String): Timestamp =
    Timestamp.from(DateTime.parse(s).toDate.toInstant)

  class ChangeHandler(sequence: Int) extends DefaultHandler {
    val changeSeq = Queue.empty[Change]
    var action: Actions.Action = Actions.Delete
    var working: Change = null
    override def startElement(uri: String, localName: String, qName: String, attributes: sax.Attributes) = {
      val attrs =
        (for {
           i <- Range(0, attributes.getLength).toSeq
         } yield (attributes.getQName(i) -> attributes.getValue(i))).toMap
      qName.toLowerCase match {
        case label if Set("create", "delete", "modify").contains(label) =>
          action = Actions.fromString(qName)
        case label if Set("node", "way", "relation").contains(label) =>
          working = Change(attrs("id").toLong,
                           qName,
                           Map.empty,
                           attrs.get("lat").map(_.toDouble),
                           attrs.get("lon").map(_.toDouble),
                           None,
                           None,
                           attrs.get("changeset").map(_.toLong).getOrElse(-1L),
                           stringToTimestamp(attrs.getOrElse("timestamp", "1970-01-01T00:00:00Z")),
                           attrs.get("uid").map(_.toLong).getOrElse(-1L),
                           attrs.getOrElse("user", ""),
                           attrs.get("version").map(_.toLong).getOrElse(-1L),
                           action != Actions.Delete,
                           sequence
                         )
        case "tag" =>
          val existing = working.tags
          working = working.copy(tags = existing ++ Map(attrs("k") -> attrs("v")))
        case "nd" =>
          val nd = Nd(attrs("ref").toLong)
          val nds = working.nds match {
            case None => Seq(nd)
            case Some(seq) => seq :+ nd
          }
          working = working.copy(nds = Some(nds))
        case "member" =>
          val member = Member(Member.typeFromString(attrs("type")), attrs("ref").toLong, attrs("role"))
          val mems = working.members match {
            case None => Seq(member)
            case Some(seq) => seq :+ member
          }
          working = working.copy(members = Some(mems))
        case "osmchange" => () // no-op
      }
    }
    override def endElement(uri: String, localName: String, qName: String) = {
      if (Set("node", "way", "relation").contains(qName.toLowerCase)) {
        changeSeq.enqueue(working)
        working = null
      }
    }
  }

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
