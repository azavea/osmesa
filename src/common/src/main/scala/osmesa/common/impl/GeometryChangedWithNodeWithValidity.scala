package osmesa.common.impl
import java.sql.Timestamp

import osmesa.common.traits

final case class GeometryChangedWithNodeWithValidity(
    geometryChanged: Boolean,
    updated: Timestamp,
    validUntil: Option[Timestamp],
    visible: Boolean,
    lat: Option[Double],
    lon: Option[Double],
    tags: Map[String, String],
    changeset: Long,
    uid: Long,
    user: String,
    id: Long,
    version: Int
) extends traits.GeometryChanged
    with traits.Node
    with traits.Validity
