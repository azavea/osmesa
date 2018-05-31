package osmesa.analytics.updater

import java.sql.Timestamp
import java.time.Instant

import geotrellis.vectortile.Layer
import org.apache.log4j.Logger
import osmesa.analytics.updater.Implicits._

trait Schema {
  val layer: Layer
  val features: Map[String, (Option[AugmentedDiffFeature], AugmentedDiffFeature)]

  val newFeatures: Seq[VTFeature]
  lazy val replacementFeatures: Seq[VTFeature] = Seq.empty[VTFeature]
  lazy val retainedFeatures: Seq[VTFeature] = Seq.empty[VTFeature]

  protected lazy val logger: Logger = Logger.getLogger(getClass)

  protected lazy val touchedFeatures: Map[String, Seq[VTFeature]] =
    Map.empty[String, Seq[VTFeature]]

  protected lazy val versionInfo: Map[String, (Int, Int, Timestamp)] =
    touchedFeatures
      .mapValues(_.last)
      .mapValues(
        f =>
          (
            f.data("__version").toInt,
            f.data("__minorVersion").toInt,
            Timestamp.from(Instant.ofEpochMilli(f.data("__updated")))
        ))

  protected lazy val minorVersions: Map[String, Int] =
    features
      .mapValues {
        case (_, curr) => curr.data
      }
      .map {
        case (id, f) =>
          versionInfo.get(id) match {
            case Some((prevVersion, _, _)) if prevVersion < f.version => (id, 0)
            case Some((prevVersion, prevMinorVersion, _)) if prevVersion == f.version =>
              (id, prevMinorVersion + 1)
            case _ => (id, 0)
          }
      }
}

trait SchemaBuilder {
  val layerName: String

  def apply(layer: Layer,
            features: Map[String, (Option[AugmentedDiffFeature], AugmentedDiffFeature)]): Schema
}
