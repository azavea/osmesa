package osmesa.common.impl
import java.sql.Timestamp

import com.vividsolutions.jts.geom.Geometry
import osmesa.common.traits

final case class MetadataWithOSMFeatureWithTimestampWithVisibility(
    timestamp: Timestamp,
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
    with traits.Timestamp
    with traits.Visibility
