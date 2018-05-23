package osmesa.common.model

import java.sql.Timestamp

import org.joda.time.DateTime

case class ChangesetComment(date: Timestamp, user: String, uid: Long, body: String)

object ChangesetComment {
  implicit def stringToTimestamp(s: String): Timestamp =
    Timestamp.from(DateTime.parse(s).toDate.toInstant)

  def fromXML(node: scala.xml.Node): ChangesetComment = {
    val date = node \@ "date"
    val user = node \@ "user"
    val uid = (node \@ "uid").toLong
    val body = (node \ "text").text

    ChangesetComment(date, user, uid, body)
  }
}
