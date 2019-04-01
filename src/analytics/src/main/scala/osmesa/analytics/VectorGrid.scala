package osmesa.analytics

import java.io.ByteArrayInputStream
import java.net.URI
import java.util.zip.GZIPInputStream

import geotrellis.proj4.WebMercator
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vector.{Extent, PointFeature}
import geotrellis.vectortile.{Layer, VInt64, VectorTile}
import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging
import osmesa.analytics.updater.Implicits._
import osmesa.analytics.updater._

import scala.collection.GenMap
import scala.collection.parallel.TaskSupport

trait VectorGrid extends Logging {
  // Default base zoom (highest resolution tiles produced)
  val DefaultBaseZoom: Int = 10

  // Number of cells per side in a gridded tile
  implicit val Cells: Int = 128

  // Number of cells in a gridded tile at the base of the pyramid (may be used for over-zooming)
  val BaseCells: Int = Cells

  // Default upload concurrency
  val DefaultUploadConcurrency: Int = 8

  implicit val LayoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator)
  val SequenceLayerName: String = "__sequences__"

  def getCommittedSequences(tile: VectorTile): Set[Int] =
    // NOTE when working with hashtags, this should be the changeset sequence, since changes from a
    // single sequence may appear in different batches depending on when changeset metadata arrives
    tile.layers
      .get(SequenceLayerName)
      .map(_.features.flatMap(f => f.data.values.map(valueToLong).map(_.intValue)))
      .map(_.toSet)
      .getOrElse(Set.empty[Int])

  def makeSequenceLayer(sequences: Set[Int], extent: Extent): (String, Layer) = {
    // create a second layer w/ a feature corresponding to committed sequences (in the absence of
    // available tile / layer metadata)
    val updatedSequences =
      sequences.zipWithIndex.map {
        case (seq, idx) =>
          idx.toString -> VInt64(seq)
      }.toMap

    val sequenceFeature = PointFeature(extent.center, updatedSequences)

    makeLayer(SequenceLayerName, extent, Seq(sequenceFeature))
  }

  def loadMVTs(urls: Map[URI, Extent])(
      implicit taskSupport: TaskSupport): GenMap[URI, VectorTile] = {
    // convert to a parallel collection to load more tiles concurrently
    val parUrls = urls.par
    parUrls.tasksupport = taskSupport

    parUrls.map {
      case (uri, extent) =>
        (uri,
         read(uri).map(
           bytes =>
             VectorTile.fromBytes(
               IOUtils.toByteArray(new GZIPInputStream(new ByteArrayInputStream(bytes))),
               extent)))
    } filter {
      case (_, mvt) => mvt.isDefined
    } map {
      case (uri, mvt) => uri -> mvt.get
    }
  }
}
