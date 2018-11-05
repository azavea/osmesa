package osmesa.common.impl
import java.sql.Timestamp

import com.vividsolutions.jts.geom.Point
import osmesa.common.traits

final case class MetadataWithOSMFeatureOfPointWithTimestampWithVisibility(
    timestamp: Timestamp,
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
    with traits.Timestamp
    with traits.Visibility
