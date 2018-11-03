package osmesa.common.impl
import java.sql.Timestamp

import osmesa.common.traits

final case class ChangesetWithTags(
    id: Long,
    createdAt: Timestamp,
    closedAt: Option[Timestamp],
    open: Boolean,
    numChanges: Int,
    minLat: Option[Double],
    maxLat: Option[Double],
    minLon: Option[Double],
    maxLon: Option[Double],
    commentsCount: Int,
    tags: Map[String, String],
    uid: Long,
    user: String
) extends traits.Changeset
    with traits.Tags
