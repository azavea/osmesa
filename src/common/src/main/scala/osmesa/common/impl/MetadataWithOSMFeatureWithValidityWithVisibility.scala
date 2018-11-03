package osmesa.common.impl
import java.sql.Timestamp

import com.vividsolutions.jts.geom.Geometry
import osmesa.common.traits

final case class MetadataWithOSMFeatureWithValidityWithVisibility(
    updated: Timestamp,
    validUntil: Option[Timestamp],
    visible: Boolean,
    `type`: Byte,
    id: Long,
    version: Int,
    changeset: Long,
    geom: Geometry,
    uid: Long,
    user: String
) extends traits.Metadata
    with traits.OSMFeature
    with traits.Validity
    with traits.Visibility
