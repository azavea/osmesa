package osmesa.common.impl
import com.vividsolutions.jts.geom.Geometry
import osmesa.common.traits

final case class MetadataWithOSMFeatureWithVisibility(
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
    with traits.Visibility
