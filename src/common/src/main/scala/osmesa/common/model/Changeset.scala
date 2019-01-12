package osmesa.common.model

import java.sql.Timestamp

import org.joda.time.DateTime

import scala.util.Try

case class Changeset(id: Long,
                     tags: Map[String, String],
                     createdAt: Timestamp,
                     open: Boolean,
                     closedAt: Option[Timestamp],
                     commentsCount: Int,
                     minLat: Option[Double],
                     maxLat: Option[Double],
                     minLon: Option[Double],
                     maxLon: Option[Double],
                     numChanges: Int,
                     uid: Long,
                     user: String,
                     comments: Seq[ChangesetComment],
                     sequence: Int)

object Changeset {
  implicit def stringToTimestamp(s: String): Timestamp =
    Timestamp.from(DateTime.parse(s).toDate.toInstant)

  implicit def stringToOptionalTimestamp(s: String): Option[Timestamp] =
    s match {
      case "" => None
      case ts => Some(ts)
    }

  implicit def stringToOptionalDouble(s: String): Option[Double] =
    s match {
      case "" => None
      case c  => Some(c.toDouble)
    }

  def fromXML(node: scala.xml.Node, sequence: Int): Changeset = {
    val id = (node \@ "id").toLong
    // Old changesets lack the appropriate field
    val commentsCount = Try((node \@ "comments_count").toInt).toOption.getOrElse(0)
    val uid = (node \@ "uid").toLong
    val user = node \@ "user"
    val numChanges = Try((node \@ "num_changes").toInt).toOption.getOrElse(0)
    val open = (node \@ "open").toBoolean
    val closedAt = node \@ "closed_at"
    val createdAt = node \@ "created_at"

    val maxLon = node \@ "max_lon"
    val minLon = node \@ "min_lon"
    val maxLat = node \@ "max_lon"
    val minLat = node \@ "min_lon"
    val tags =
      (node \ "tag").map(tag => (tag \@ "k", tag \@ "v")).toMap
    val comments = (node \ "discussion" \ "comment").map(ChangesetComment.fromXML)

    Changeset(
      id,
      tags,
      createdAt,
      open,
      closedAt,
      commentsCount,
      minLat,
      maxLat,
      minLon,
      maxLon,
      numChanges,
      uid,
      user,
      comments,
      sequence
    )
  }
}
