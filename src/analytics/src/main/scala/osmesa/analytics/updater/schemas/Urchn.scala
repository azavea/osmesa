package osmesa.analytics.updater.schemas

import geotrellis.vector.{Feature, Geometry}
import geotrellis.vectortile._
import osmesa.analytics.updater.Implicits._
import osmesa.analytics.updater._

class Urchn(
    override val layer: Layer,
    override val features: Map[String, (Option[AugmentedDiffFeature], AugmentedDiffFeature)])
    extends Schema {
  override protected lazy val touchedFeatures: Map[String, Seq[MVTFeature[Geometry]]] = {
    val featureIds = features.keySet

    layer.features
      .filter(f => featureIds.contains(f.data("__id")))
      .groupBy(f => f.data("__id"): String)
      .mapValues(
        fs =>
          fs.sortWith(_.data("__minorVersion") < _.data("__minorVersion"))
            .sortWith(_.data("__version") < _.data("__version")))
  }

  private lazy val authors: Map[String, Set[String]] =
    touchedFeatures
      .mapValues(_.last)
      .mapValues(_.data("__authors").split(",").toSet)

  private lazy val creation: Map[String, Long] =
    touchedFeatures
      .mapValues(_.head)
      .mapValues(_.data("__creation"))

  lazy val newFeatures: Seq[MVTFeature[Geometry]] =
    features
      .filter {
        case (id, (_, curr)) =>
          versionInfo.get(id) match {
            case Some((_, _, prevTimestamp)) if curr.data.timestamp.after(prevTimestamp) => true
            case None                                                                    => true
            case _                                                                       => false
          }
      }
      .values
      .filter {
        // filter out null geometries
        case (_, curr) => Option(curr.geom).isDefined && curr.isValid
      }
      .map {
        case (_, curr) =>
          // NOTE: if this feature appears in the current tile for the first time, creation, authors, and minorVersions
          // will be incomplete (and therefore wrong)
          makeFeature(
            curr,
            creation
              .getOrElse(curr.data.elementId, curr.data.timestamp.getTime),
            authors
              .get(curr.data.elementId)
              .map(_ + curr.data.user)
              .getOrElse(Set(curr.data.user)),
            minorVersions.get(curr.data.elementId)
          )
      }
      .filter(_.isDefined)
      .map(_.get)
      .toSeq

  private def makeFeature(feature: AugmentedDiffFeature,
                          creation: Long,
                          authors: Set[String],
                          minorVersion: Option[Int]): Option[MVTFeature[Geometry]] = {
    val id = feature.data.id

    val elementId = feature.data.`type` match {
      case "node"     => s"n$id"
      case "way"      => s"w$id"
      case "relation" => s"r$id"
      case _          => id.toString
    }

    feature match {
      case _ if Option(feature.geom).isDefined && feature.geom.isValid =>
        Some(
          MVTFeature(
            feature.geom, // when features are deleted, this will be the last geometry that was visible
            feature.data.tags.map {
              case (k, v) => (k, VString(v))
            } ++ Map(
              "__id" -> VString(elementId),
              "__changeset" -> VInt64(feature.data.changeset),
              "__updated" -> VInt64(feature.data.timestamp.getTime),
              "__version" -> VInt64(feature.data.version),
              "__vtileGen" -> VInt64(System.currentTimeMillis),
              "__creation" -> VInt64(creation),
              "__authors" -> VString(authors.mkString(",")),
              "__lastAuthor" -> VString(feature.data.user)
            ) ++ minorVersion
              .map(v => Map("__minorVersion" -> VInt64(v)))
              .getOrElse(Map.empty[String, Value])
          )
        )
      case _ => None
    }
  }
}

object Urchn extends SchemaBuilder {
  override val layerName: String = "history"

  def apply(layer: Layer,
            features: Map[String, (Option[AugmentedDiffFeature], AugmentedDiffFeature)]) =
    new Urchn(layer, features)
}
