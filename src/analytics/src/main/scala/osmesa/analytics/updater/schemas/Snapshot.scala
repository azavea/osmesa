package osmesa.analytics.updater.schemas

import geotrellis.vector.Feature
import geotrellis.vectortile.{Layer, VInt64, VString}
import osmesa.analytics.updater._

class Snapshot(
    override val layer: Layer,
    override val features: Map[String, (Option[AugmentedDiffFeature], AugmentedDiffFeature)])
    extends Schema {
  lazy val newFeatures: Seq[VTFeature] =
    features.values
      .map(_._2)
      .filter(_.data.visible.getOrElse(true))
      .map(makeFeature)
      .filter(_.isDefined)
      .map(_.get)
      .toSeq

  private def makeFeature(feature: AugmentedDiffFeature): Option[VTFeature] = {
    val id = feature.data.id

    val elementId = feature.data.`type` match {
      case "node"     => s"n$id"
      case "way"      => s"w$id"
      case "relation" => s"r$id"
      case _          => id.toString
    }

    feature match {
      case _ if feature.geom.isValid =>
        Some(
          Feature(
            feature.geom,
            feature.data.tags.map {
              case (k, v) => (k, VString(v))
            } ++ Map(
              "__id" -> VString(elementId),
              "__changeset" -> VInt64(feature.data.changeset),
              "__updated" -> VInt64(feature.data.timestamp.getTime),
              "__version" -> VInt64(feature.data.version),
              "__uid" -> VInt64(feature.data.uid),
              "__user" -> VString(feature.data.user)
            )
          )
        )
      case _ => None
    }
  }
}

object Snapshot extends SchemaBuilder {
  override val layerName: String = "data"

  def apply(layer: Layer,
            features: Map[String, (Option[AugmentedDiffFeature], AugmentedDiffFeature)]) =
    new Snapshot(layer, features)
}
