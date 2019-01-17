package osmesa.common.model

import java.sql.Timestamp

import org.joda.time.DateTime
import org.xml.sax
import org.xml.sax.helpers.DefaultHandler

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
  implicit def stringToTimestamp(s: String): Timestamp =
    Timestamp.from(DateTime.parse(s).toDate.toInstant)

  class ChangeHandler(sequence: Int) extends DefaultHandler {
    final val ActionLabels: Set[String] = Set("create", "delete", "modify")
    final val ElementLabels: Set[String] = Set("node", "way", "relation")

    private val changeSeq: ListBuffer[Change] = ListBuffer.empty
    private val tags: mutable.Map[String, String] = mutable.Map.empty
    private val nds: ListBuffer[Nd] = ListBuffer.empty
    private val members: ListBuffer[Member] = ListBuffer.empty
    private var action: Actions.Action = _
    private var attrs: Map[String, String] = _

    def changes: Seq[Change] = changeSeq

    override def startElement(uri: String,
                              localName: String,
                              qName: String,
                              attributes: sax.Attributes): Unit = {
      val attrs =
        (for {
          i <- Range(0, attributes.getLength)
        } yield attributes.getQName(i) -> attributes.getValue(i)).toMap

      qName.toLowerCase match {
        case label if ActionLabels.contains(label) =>
          action = Actions.fromString(qName)

        case label if ElementLabels.contains(label) =>
          reset()

          this.attrs = attrs

        case "tag" =>
          tags.update(attrs("k"), attrs("v"))

        case "nd" =>
          nds.append(Nd(attrs("ref").toLong))

        case "member" =>
          members.append(
            Member(Member.typeFromString(attrs("type")), attrs("ref").toLong, attrs("role")))

        case _ => () // no-op
      }
    }

    def reset(): Unit = {
      tags.clear()
      nds.clear()
      members.clear()
    }

    override def endElement(uri: String, localName: String, qName: String): Unit = {
      if (ElementLabels.contains(qName.toLowerCase)) {
        changeSeq.append(
          Change(
            attrs("id").toLong,
            qName,
            tags.toMap,
            attrs.get("lat").map(_.toDouble),
            attrs.get("lon").map(_.toDouble),
            Option(nds).filter(_.nonEmpty),
            Option(members).filter(_.nonEmpty),
            attrs.get("changeset").map(_.toLong).getOrElse(-1L),
            stringToTimestamp(attrs.getOrElse("timestamp", "1970-01-01T00:00:00Z")),
            attrs.get("uid").map(_.toLong).getOrElse(-1L),
            attrs.getOrElse("user", ""),
            attrs.get("version").map(_.toLong).getOrElse(-1L),
            action != Actions.Delete,
            sequence
          ))
      }
    }
  }
}
