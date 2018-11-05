package osmesa.common.impl
import java.sql.Timestamp

import com.vividsolutions.jts.geom.Geometry
import osmesa.common.traits

final case class GeometryChangedWithMinorVersionWithOSMFeatureOfGeometryWithTagsWithValidity(
    geometryChanged: Boolean,
    minorVersion: Int,
    tags: Map[String, String],
    updated: Timestamp,
    validUntil: Option[Timestamp],
    `type`: Byte,
    id: Long,
    version: Int,
    changeset: Long,
    geom: Geometry
) extends traits.GeometryChanged
    with traits.MinorVersion
    with traits.OSMFeature[Geometry]
    with traits.Tags
    with traits.Validity
