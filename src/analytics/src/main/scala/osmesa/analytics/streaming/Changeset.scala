package osmesa.analytics.streaming

import org.joda.time.DateTime

class Changeset(val id: Long,
                val createdAt: DateTime,
                val closedAt: Option[DateTime],
                val open: Boolean,
                val numChanges: Int,
                val user: String,
                val uid: Long,
                val minLat: Option[Float],
                val maxLat: Option[Float],
                val minLon: Option[Float],
                val maxLon: Option[Float],
                val commentsCount: Int,
                val tags: Map[String, String])

object Changeset {
  implicit def stringToDateTime(s: String): DateTime =
    DateTime.parse(s)

  implicit def stringToOptionalDateTime(s: String): Option[DateTime] =
    s match {
      case "" => None
      case ts => Some(ts)
    }

  implicit def stringToOptionalFloat(s: String): Option[Float] =
    s match {
      case "" => None
      case c  => Some(c.toFloat)
    }

  def fromXML(node: scala.xml.Node): Changeset = {
    val id = (node \@ "id").toLong
    val commentsCount = (node \@ "comments_count").toInt
    val uid = (node \@ "uid").toLong
    val user = node \@ "user"
    val numChanges = (node \@ "num_changes").toInt
    val open = (node \@ "open").toBoolean
    val closedAt = node \@ "closed_at"
    val createdAt = node \@ "created_at"

    val maxLon = node \@ "max_lon"
    val minLon = node \@ "min_lon"
    val maxLat = node \@ "max_lon"
    val minLat = node \@ "min_lon"
    val tags =
      (node \ "tag").map(tag => (tag \@ "k", tag \@ "v")).toMap

    new Changeset(id,
                  createdAt,
                  closedAt,
                  open,
                  numChanges,
                  user,
                  uid,
                  minLat,
                  maxLat,
                  minLon,
                  maxLon,
                  commentsCount,
                  tags)
  }
}
