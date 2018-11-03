package osmesa.common.impl
import java.sql.Timestamp

import osmesa.common.traits

final case class TimestampWithWay(
    timestamp: Timestamp,
    visible: Boolean,
    tags: Map[String, String],
    changeset: Long,
    nds: Seq[Long],
    uid: Long,
    user: String,
    id: Long,
    version: Int
) extends traits.Timestamp
    with traits.Way
