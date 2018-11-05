package osmesa.common.impl
import com.vividsolutions.jts.geom.Point
import osmesa.common.traits

final case class MetadataWithOSMFeatureOfPointWithVisibility(
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
    with traits.Visibility
