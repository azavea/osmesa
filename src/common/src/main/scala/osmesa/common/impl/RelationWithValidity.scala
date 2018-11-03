package osmesa.common.impl
import java.sql.Timestamp

import osmesa.common.traits

final case class RelationWithValidity(
    updated: Timestamp,
    validUntil: Option[Timestamp],
    visible: Boolean,
    tags: Map[String, String],
    changeset: Long,
    uid: Long,
    user: String,
    members: Seq[traits.Member],
    id: Long,
    version: Int
) extends traits.Relation
    with traits.Validity
