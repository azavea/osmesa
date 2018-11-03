package osmesa.common.impl
import osmesa.common.traits.{Node, Validity}

final case class NodeWithValidity(
    id: Long,
    tags: Map[String, String],
    lat: Option[Double],
    lon: Option[Double],
    changeset: Long,
    updated: java.sql.Timestamp,
    validUntil: Option[java.sql.Timestamp],
    uid: Long,
    user: String,
    version: Int,
    visible: Boolean
) extends Node
    with Validity
