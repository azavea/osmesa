package osmesa.common.impl
import java.sql.Timestamp

import com.vividsolutions.jts.geom.Point
import osmesa.common.traits

final case class MetadataWithOSMFeatureOfPointWithValidityWithVisibility(
    updated: Timestamp,
    validUntil: Option[Timestamp],
    visible: Boolean,
    `type`: Byte,
    id: Long,
    version: Int,
    changeset: Long,
    geom: Point,
    uid: Long,
    user: String
) extends traits.Metadata
    with traits.OSMFeature[Point]
    with traits.Validity
    with traits.Visibility
