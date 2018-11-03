package osmesa.common.impl

import osmesa.common.traits.{Node, Timestamp}

final case class NodeWithTimestamp(
    id: Long,
    tags: Map[String, String],
    lat: Option[Double],
    lon: Option[Double],
    changeset: Long,
    timestamp: java.sql.Timestamp,
    uid: Long,
    user: String,
    version: Int,
    visible: Boolean
) extends Node
    with Timestamp
