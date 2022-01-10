package osmesa.analytics.updater.schemas

import java.sql.Timestamp
import java.time.Instant

import geotrellis.vector.Geometry
import geotrellis.vectortile._
import osmesa.analytics.updater.Implicits._
import osmesa.analytics.updater._

class History(
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
      .filter {
        // filter out null geometries
        case (_, (_, curr)) => Option(curr.geom).isDefined && curr.isValid
      }
      .map {
        case (id, (_, curr)) => (id, makeFeature(curr, minorVersions.get(id)))
      }
      .values
      .filter(_.isDefined)
      .map(_.get)
      .toSeq

  override lazy val replacementFeatures: Seq[MVTFeature[Geometry]] = {
    val activeFeatures = touchedFeatures
      .filter {
        case (id, fs) =>
          features(id)._2.data.timestamp
            .after(Timestamp.from(Instant.ofEpochMilli(fs.last.data("__updated"))))
      }

    val featuresToReplace = activeFeatures
      .mapValues(fs => fs.filter(_.data("__validUntil").toLong == 0))
      .values
      .flatten
      .toSeq

    val replacedFeatures = featuresToReplace
      .map(f => updateFeature(f, features(f.data("__id"))._2.data.timestamp))

    logger.info(s"Rewriting ${replacedFeatures.length.formatted("%,d")} features")

    replacedFeatures
  }

  override lazy val retainedFeatures: Seq[MVTFeature[Geometry]] = {
    val activeFeatures = touchedFeatures
      .filter {
        case (id, fs) =>
          features(id)._2.data.timestamp
            .after(Timestamp.from(Instant.ofEpochMilli(fs.last.data("__updated"))))
      }

    activeFeatures
      .mapValues(fs => fs.filterNot(_.data("__validUntil").toLong == 0))
      .values
      .flatten
      .toSeq
  }

  private def makeFeature(feature: AugmentedDiffFeature,
                          minorVersion: Option[Int],
                          validUntil: Option[Long] = None): Option[MVTFeature[Geometry]] = {
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
          MVTFeature(
            feature.geom, // when features are deleted, this will be the last geometry that was visible
            feature.data.tags.map {
              case (k, v) => (k, VString(v))
            } ++ Map(
              "__id" -> VString(elementId),
              "__changeset" -> VInt64(feature.data.changeset),
              "__updated" -> VInt64(feature.data.timestamp.getTime),
              "__validUntil" -> VInt64(validUntil.getOrElse(0L)),
              "__version" -> VInt64(feature.data.version),
              "__uid" -> VInt64(feature.data.uid),
              "__user" -> VString(feature.data.user),
              "__visible" -> VBool(feature.data.visible.getOrElse(true))
            ) ++ minorVersion
              .map(v => Map("__minorVersion" -> VInt64(v)))
              .getOrElse(Map.empty[String, Value])
          )
        )
      case _ => None
    }
  }

  private def updateFeature(feature: MVTFeature[Geometry], validUntil: Timestamp): MVTFeature[Geometry] = {
    MVTFeature(
      feature.geom,
      feature.data.updated("__validUntil", VInt64(validUntil.getTime))
    )
  }
}

object History extends SchemaBuilder {
  override val layerName: String = "all"

  def apply(layer: Layer,
            features: Map[String, (Option[AugmentedDiffFeature], AugmentedDiffFeature)]) =
    new History(layer, features)
}
