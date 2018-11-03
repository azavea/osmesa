package osmesa.common.impl

import osmesa.common.traits
import osmesa.common.traits.Member

final case class OSM(
    id: Long,
    `type`: Byte,
    tags: Map[String, String],
    lat: Option[Double],
    lon: Option[Double],
    nds: Seq[Long],
    members: Seq[Member],
    changeset: Long,
    timestamp: java.sql.Timestamp,
    uid: Long,
    user: String,
    version: Int,
    visible: Boolean
) extends traits.OSM
