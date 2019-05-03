package osmesa.analytics

import java.net.{URI, URLEncoder}
import java.nio.charset.StandardCharsets

import geotrellis.raster._
import geotrellis.raster.resample.Sum
import geotrellis.spark.io.index.zcurve.ZSpatialKeyIndex
import geotrellis.spark.{KeyBounds, SpatialKey}
import geotrellis.vector.{Extent, Feature, Point, PointFeature, Geometry => GTGeometry}
import geotrellis.vectortile.{VInt64, Value, VectorTile}
import org.apache.spark.sql._
import osmesa.analytics.vectorgrid._
import osmesa.analytics.updater.Implicits._
import osmesa.analytics.updater.{makeLayer, path, write}
import osmesa.common.raster._

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.{ForkJoinTaskSupport, TaskSupport}
import scala.collection.{GenIterable, GenMap}
import scala.concurrent.forkjoin.ForkJoinPool

object Footprints extends VectorGrid {
  override val DefaultBaseZoom: Int = 14
  override val BaseCells: Int = Cells * 4

  import Implicits._
  import implicits._

  def create(nodes: DataFrame, tileSource: URI, baseZoom: Int = DefaultBaseZoom)(
      implicit concurrentUploads: Option[Int] = None): DataFrame = {
    import nodes.sparkSession.implicits._

    val points = nodes
      .repartition() // eliminate skew
      .as[PointWithKey]

    points
      .tile(baseZoom)
      .rasterize(BaseCells)
      .pyramid(baseZoom)
      .mapPartitions { tiles: Iterator[RasterTileWithKey] =>
        // increase the number of concurrent network-bound tasks
        implicit val taskSupport: ForkJoinTaskSupport = new ForkJoinTaskSupport(
          new ForkJoinPool(concurrentUploads.getOrElse(DefaultUploadConcurrency)))

        try {
          // TODO in the future, allow tiles to contain layers for multiple keys; this has knock-on effects
          updateTiles(tileSource, Map.empty[URI, VectorTile], tiles.vectorize).iterator
        } finally {
          taskSupport.environment.shutdown()
        }
      }
      .toDF("count", "z", "x", "y", "key")
  }

  def updateTiles(
      tileSource: URI,
      mvts: GenMap[URI, VectorTile],
      tiles: TraversableOnce[
        (String, Int, SpatialKey, Extent, ArrayBuffer[PointFeature[(Long, Int)]], List[Int])])(
      implicit taskSupport: TaskSupport): GenIterable[(Int, Int, Int, Int, String)] = {
    // parallelize tiles to facilitate greater upload parallelism
    val parTiles = tiles.toIterable.par
    parTiles.tasksupport = taskSupport

    parTiles.map {
      // update tiles
      case (key, z, sk, extent, feats, sequences) =>
        val uri = makeURI(tileSource, key, z, sk)

        mvts.get(uri) match {
          case Some(tile) =>
            // update existing tiles

            // load the target layer
            val layers = tile.layers.get(key) match {
              case Some(layer) =>
                // TODO feature construction / updating is very similar to osmesa.analytics.updater.schemas.*; see if the 2
                // can be merged

                val newFeaturesById: Map[Long, Feature[GTGeometry, (Long, Int)]] =
                  feats
                    .groupBy(_.data._1)
                    .mapValues(_.head)
                val featureIds: Set[Long] = newFeaturesById.keySet

                val existingFeatures: Set[Long] =
                  layer.features.map(f => f.data("id"): Long).toSet

                val unmodifiedFeatures =
                  layer.features.filterNot(f => featureIds.contains(f.data("id")))

                val modifiedFeatures =
                  layer.features.filter(f => featureIds.contains(f.data("id")))

                val replacementFeatures: Seq[Feature[GTGeometry, Map[String, Value]]] =
                  modifiedFeatures.map { f =>
                    f.mapData { d =>
                      val prevDensity: Long = d("density")
                      d.updated("density", VInt64(prevDensity + newFeaturesById(d("id")).data._2))
                    }
                  }

                val newFeatures: Seq[Feature[GTGeometry, Map[String, Value]]] =
                  makeFeatures(
                    feats
                      .filterNot(f => existingFeatures.contains(f.data._1)))

                unmodifiedFeatures ++ replacementFeatures ++ newFeatures match {
                  case updatedFeatures if (replacementFeatures.length + newFeatures.length) > 0 =>
                    val updatedLayer = makeLayer(key, extent, updatedFeatures)
                    val sequenceLayer =
                      makeSequenceLayer(getCommittedSequences(tile) ++ sequences.toSet, extent)

                    Some(updatedLayer, sequenceLayer)
                  case _ =>
                    logError(s"No changes to $uri; THIS SHOULD NOT HAVE HAPPENED.")
                    None
                }
              case None =>
                Some(makeLayer(key, extent, makeFeatures(feats)),
                     makeSequenceLayer(sequences.toSet, extent))
            }

            layers match {
              case Some((dataLayer, sequenceLayer)) =>
                // merge all available layers into a new tile
                val newTile =
                  VectorTile(
                    tile.layers
                      .updated(dataLayer._1, dataLayer._2)
                      // update a second layer with a feature corresponding to committed sequences
                      .updated(sequenceLayer._1, sequenceLayer._2),
                    extent
                  )

                write(newTile, uri)

              case None => // no new data
            }

          case None =>
            val tile = VectorTile(Map(makeLayer(key, extent, makeFeatures(feats)),
                                      makeSequenceLayer(sequences.toSet, extent)),
                                  extent)

            write(tile, uri)
        }

        (feats.size, z, sk.col, sk.row, key)
    }
  }

  def makeFeatures(
      features: Seq[PointFeature[(Long, Int)]]): Seq[Feature[Point, Map[String, VInt64]]] =
    features.map(f =>
      f.mapData {
        case (id, density) =>
          Map("id" -> VInt64(id), "density" -> VInt64(density))
    })

  def makeURI(tileSource: URI, key: String, zoom: Int, sk: SpatialKey): URI = {
    val filename =
      s"${URLEncoder.encode(key, StandardCharsets.UTF_8.toString)}/${path(zoom, sk)}"
    tileSource.resolve(filename)
  }

  def update(nodes: DataFrame, tileSource: URI, baseZoom: Int = DefaultBaseZoom)(
      implicit concurrentUploads: Option[Int] = None): DataFrame = {
    import nodes.sparkSession.implicits._

    val points = nodes
      .repartition() // eliminate skew
      .as[PointWithKeyAndSequence]

    points
      .tile(baseZoom)
      .rasterize(BaseCells)
      .pyramid(baseZoom)
      .groupByKeyAndTile()
      .mapPartitions { rows: Iterator[RasterWithSequenceTileSeqWithTileCoordinatesAndKey] =>
        // materialize the iterator so that its contents can be used multiple times
        val tiles = rows.toVector

        // increase the number of concurrent network-bound tasks
        implicit val taskSupport: ForkJoinTaskSupport = new ForkJoinTaskSupport(
          new ForkJoinPool(concurrentUploads.getOrElse(DefaultUploadConcurrency)))

        try {
          // TODO in the future, allow tiles to contain layers for multiple keys; this has knock-on effects
          val urls = makeUrls(tileSource, tiles)
          val mvts = loadMVTs(urls)
          val uncommitted = getUncommittedTiles(tileSource, tiles, mvts)

          updateTiles(tileSource, mvts, uncommitted.merge.vectorize).iterator
        } finally {
          taskSupport.environment.shutdown()
        }
      }
      .toDF("count", "z", "x", "y", "key")
  }

  def makeUrls(tileSource: URI,
               tiles: Seq[RasterWithSequenceTileSeqWithTileCoordinatesAndKey]): Map[URI, Extent] =
    tiles.map { tile =>
      (makeURI(tileSource, tile.key, tile.zoom, tile.sk),
       tile.sk.extent(LayoutScheme.levelForZoom(tile.zoom).layout))
    } toMap

  def getUncommittedTiles(
      tileSource: URI,
      tiles: Seq[RasterWithSequenceTileSeqWithTileCoordinatesAndKey],
      mvts: GenMap[URI, VectorTile]): Seq[RasterWithSequenceTileSeqWithTileCoordinatesAndKey] =
    tiles
      .map(tile => {
        val uri = makeURI(tileSource, tile.key, tile.zoom, tile.sk)
        val committedSequences = mvts.get(uri).map(getCommittedSequences).getOrElse(Set.empty[Int])

        RasterWithSequenceTileSeqWithTileCoordinatesAndKey(
          tile.tiles.filterNot(t => committedSequences.contains(t.sequence)),
          tile.zoom,
          tile.sk,
          tile.key)
      })
      .filterNot(tileSeq => tileSeq.tiles.isEmpty)

  def merge(uncommitted: Seq[RasterWithSequenceTileSeqWithTileCoordinatesAndKey])
    : Seq[(String, Int, Int, Int, Raster[Tile], List[Int])] =
    uncommitted
      .map {
        // merge tiles with different sequences together
        tile =>
          val data = tile.tiles.toList
          val sequences = data.map(_.sequence)
          val rasters = data.map(_.raster)
          val targetExtent =
            tile.sk.extent(LayoutScheme.levelForZoom(tile.zoom).layout)

          val newTile = rasters.foldLeft(MutableSparseIntTile(Cells, Cells): Tile)((acc, raster) =>
            acc.merge(targetExtent, raster.extent, raster.tile, Sum))

          // TODO provide a case class for this
          (tile.key,
           tile.zoom,
           tile.sk.col,
           tile.sk.row,
           (newTile, targetExtent): Raster[Tile],
           sequences)
      }

  /**
    * Group tiles by key and tile coordinates.
    *
    * @param tiles Tiles to group.
    * @return Tiles grouped by key and tile coordinates.
    */
  def groupByKeyAndTile(tiles: Dataset[RasterTileWithKeyAndSequence])
    : Dataset[RasterWithSequenceTileSeqWithTileCoordinatesAndKey] = {
    import tiles.sparkSession.implicits._

    tiles
      .groupByKey(tile => (tile.key, tile.zoom, tile.sk))
      .mapGroups {
        case ((k, z, sk), tiles: Iterator[RasterTileWithKeyAndSequence]) =>
          RasterWithSequenceTileSeqWithTileCoordinatesAndKey(
            tiles
              .map(x => RasterWithSequence(x.raster, x.sequence))
              .toVector, // this CANNOT be toSeq, as that produces a stream, which is subsequently partially consumed
            z,
            sk,
            k
          )
      }
  }

  object implicits {
    implicit class FootprintsExtendedRasterTileWithKeyAndSequence(
        val rasterTiles: Dataset[RasterTileWithKeyAndSequence]) {
      def groupByKeyAndTile(): Dataset[RasterWithSequenceTileSeqWithTileCoordinatesAndKey] =
        Footprints.groupByKeyAndTile(rasterTiles)
    }

    implicit class ExtendedTileCoordinatesWithKeyAndRasterWithSequenceTileSeq(
        uncommitted: Seq[RasterWithSequenceTileSeqWithTileCoordinatesAndKey]) {
      def merge: Seq[(String, Int, Int, Int, Raster[Tile], List[Int])] =
        Footprints.merge(uncommitted)
    }

    implicit class ExtendedRasterTileWithKeySeq(tiles: TraversableOnce[RasterTileWithKey]) {
      def vectorize: TraversableOnce[
        (String, Int, SpatialKey, Extent, ArrayBuffer[PointFeature[(Long, Int)]], List[Int])] =
        tiles.map { tile =>
          // convert into features
          val raster = tile.raster
          val rasterExtent =
            RasterExtent(raster.extent, raster.tile.cols, raster.tile.rows)
          val index = new ZSpatialKeyIndex(
            KeyBounds(SpatialKey(0, 0), SpatialKey(raster.tile.cols - 1, raster.tile.rows - 1)))

          val features = ArrayBuffer[PointFeature[(Long, Int)]]()

          raster.tile.foreach { (c: Int, r: Int, value: Int) =>
            if (value > 0) {
              features.append(
                PointFeature(Point(rasterExtent.gridToMap(c, r)),
                             (index.toIndex(SpatialKey(c, r)).toLong, value)))
            }
          }

          (tile.key, tile.zoom, tile.sk, raster.extent, features, List.empty[Int])
        }
    }

    implicit class VectorizationExtension(
        tiles: Seq[(String, Int, Int, Int, Raster[Tile], List[Int])]) {
      def vectorize: Seq[
        (String, Int, SpatialKey, Extent, ArrayBuffer[PointFeature[(Long, Int)]], List[Int])] =
        tiles.map {
          // convert into features
          case (k, z, x, y, raster, sequences) =>
            val sk = SpatialKey(x, y)
            val rasterExtent =
              RasterExtent(raster.extent, raster.tile.cols, raster.tile.rows)
            val index = new ZSpatialKeyIndex(
              KeyBounds(SpatialKey(0, 0), SpatialKey(raster.tile.cols - 1, raster.tile.rows - 1)))

            val features = ArrayBuffer[PointFeature[(Long, Int)]]()

            raster.tile.foreach { (c: Int, r: Int, value: Int) =>
              if (value > 0) {
                features.append(
                  PointFeature(Point(rasterExtent.gridToMap(c, r)),
                               (index.toIndex(SpatialKey(c, r)).toLong, value)))
              }
            }

            (k, z, sk, raster.extent, features, sequences)
        }

    }
  }
}
