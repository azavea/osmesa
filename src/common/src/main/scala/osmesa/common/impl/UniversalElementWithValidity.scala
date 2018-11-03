package osmesa.common.impl
import java.sql.Timestamp

import osmesa.common.traits

final case class UniversalElementWithValidity(
    updated: Timestamp,
    validUntil: Option[Timestamp],
    visible: Boolean,
    lat: Option[Double],
    lon: Option[Double],
    tags: Map[String, String],
    changeset: Long,
    nds: Seq[Long],
    uid: Long,
    user: String,
    members: Seq[traits.Member],
    id: Long,
    version: Int
) extends traits.UniversalElement
    with traits.Validity
