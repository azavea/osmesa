package osmesa.analytics

import java.net.URI

import geotrellis.raster.RasterExtent
import geotrellis.spark.io.index.zcurve.ZSpatialKeyIndex
import geotrellis.spark.{KeyBounds, SpatialKey}
import geotrellis.vector._
import geotrellis.vectortile.{VInt64, Value, VectorTile}
import org.apache.spark.sql._
import osmesa.analytics.updater.Implicits._
import osmesa.analytics.updater.{makeLayer, path, write}
import osmesa.analytics.vectorgrid._

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.{ForkJoinTaskSupport, TaskSupport}
import scala.collection.{GenIterable, GenMap}
import scala.concurrent.forkjoin.ForkJoinPool

object EditHistogram extends VectorGrid {
  import Implicits._
  import implicits._

  def create(nodes: DataFrame, tileSource: URI, baseZoom: Int = DefaultBaseZoom)(
      implicit concurrentUploads: Option[Int] = None): DataFrame = {
    import nodes.sparkSession.implicits._

    val tiles = if (nodes.columns.contains("facets")) {
      nodes
        .repartition() // eliminate skew
        .as[PointWithKeyAndFacets]
        .tile(baseZoom)
    } else {
      nodes
        .repartition() // eliminate skew
        .as[PointWithKey]
        .tile(baseZoom)
    }

    create(tiles, tileSource, baseZoom)
  }

  def create(geometryTiles: Dataset[GeometryTileWithKey], tileSource: URI, baseZoom: Int)(
      implicit concurrentUploads: Option[Int],
      d: DummyImplicit): DataFrame = {
    import geometryTiles.sparkSession.implicits._

    geometryTiles
      .rasterize(BaseCells)
      .pyramid(baseZoom)
      .vectorize
      .groupByKey(tile => (tile.zoom, tile.sk))
      .mapGroups {
        case ((zoom, sk), tiles) =>
          VectorTileWithSequences(zoom, sk, tiles.flatMap(_.features).merge(), Set.empty[Int])
      }
      .mapPartitions { tiles: Iterator[VectorTileWithSequences] =>
        // increase the number of concurrent network-bound tasks
        implicit val taskSupport: ForkJoinTaskSupport = new ForkJoinTaskSupport(
          new ForkJoinPool(concurrentUploads.getOrElse(DefaultUploadConcurrency)))

        try {
          updateTiles(tileSource, Map.empty[URI, VectorTile], tiles).iterator
        } finally {
          taskSupport.environment.shutdown()
        }
      }
      .toDF("count", "z", "x", "y")
  }

  def updateTiles(tileSource: URI,
                  mvts: GenMap[URI, VectorTile],
                  tiles: TraversableOnce[VectorTileWithSequences])(
      implicit taskSupport: TaskSupport): GenIterable[(Int, Int, Int, Int)] = {
    // parallelize tiles to facilitate greater upload parallelism
    val parTiles = tiles.toIterable.par
    parTiles.tasksupport = taskSupport

    parTiles.map {
      // update tiles
      case VectorTileWithSequences(z, sk, feats, sequences) =>
        val extent = sk.extent(LayoutScheme.levelForZoom(z).layout)
        val uri = makeURI(tileSource, z, sk)
        val key = "edits"

        mvts.get(uri) match {
          case Some(tile) =>
            // update existing tiles

            // load the target layer
            val layers = tile.layers.get(key) match {
              case Some(layer) =>
                // TODO feature construction / updating is very similar to osmesa.analytics.updater.schemas.*; see if the 2
                // can be merged

                // nudge geometries to cover the center of a point rather than the top-left (moving them into the
                // tile's extent)
                val layerFeatures = layer.features
                    .map(f =>
                      f.mapGeom(
                        _.as[Point].get.translate(layer.resolution / 2, layer.resolution / -2)))

                val newFeaturesById: Map[Long, Feature[Geometry, Map[String, Long]]] =
                  feats
                    .groupBy(_.data("__id"))
                    .mapValues(_.head)
                val featureIds: Set[Long] = newFeaturesById.keySet

                val existingFeatures: Set[Long] =
                  layer.features.map(f => f.data("__id"): Long).toSet

                val unmodifiedFeatures =
                  layerFeatures
                    .filterNot(f => featureIds.contains(f.data("__id")))

                val modifiedFeatures =
                  layerFeatures.filter(f => featureIds.contains(f.data("__id")))

                val replacementFeatures: Seq[Feature[Geometry, Map[String, Value]]] =
                  modifiedFeatures.map(f =>
                    f.mapData(d =>
                      aggregateValues(d.mapValues(_.toLong) ++ newFeaturesById(d("__id")).data)))

                val newFeatures =
                  makeFeatures(
                    feats
                      .filterNot(f => existingFeatures.contains(f.data("__id")))).toSeq

                unmodifiedFeatures ++ replacementFeatures ++ newFeatures match {
                  case updatedFeatures if (replacementFeatures.length + newFeatures.length) > 0 =>
                    val updatedLayer = makeLayer(key, extent, updatedFeatures, Cells)
                    val sequenceLayer =
                      makeSequenceLayer(getCommittedSequences(tile) ++ sequences, extent, Cells)

                    Some(updatedLayer, sequenceLayer)
                  case _ =>
                    logError(s"No changes to $uri; THIS SHOULD NOT HAVE HAPPENED.")
                    None
                }
              case None =>
                Some(makeLayer(key, extent, makeFeatures(feats), Cells),
                     makeSequenceLayer(sequences, extent, Cells))
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
            write(VectorTile(Map(makeLayer(key, extent, makeFeatures(feats), Cells),
                                 makeSequenceLayer(sequences, extent, Cells)),
                             extent),
                  uri)
        }

        (feats.size, z, sk.col, sk.row)
    }
  }

  def makeFeatures(features: Iterable[PointFeature[Map[String, Long]]])
    : Iterable[Feature[Point, Map[String, VInt64]]] =
    features
      .map(f => f.mapData(aggregateValues))
      .toSeq
      // put recently-edited features first
      .sortBy(_.data.keys.filter(k => !k.startsWith("__")).max)
      .reverse

  def aggregateValues(data: Map[String, Long]): Map[String, VInt64] = {
    val dates = data.filterKeys(k => !k.startsWith("__") && !k.contains(":"))
    val facets =
      data.keys.filter(k => !k.startsWith("__") && k.contains(":")).map(_.split(":").last)

    // sum each facet
    val facetTotals = facets
      .flatMap(
        facet =>
          Seq(
            s"__total:${facet}" -> VInt64(
              data.filterKeys(k => !k.startsWith("__") && k.endsWith(s":${facet}")).values.sum),
            s"__lastEdit:${facet}" -> VInt64(
              data
                .filterKeys(k => !k.startsWith("__") && k.endsWith(s":${facet}"))
                .keys
                .max
                .split(":")
                .head
                .toLong)
        ))
      .toMap

    // convert values and summarize
    data.mapValues(VInt64) ++
      Map(
        // sum all edit counts
        "__total" -> VInt64(dates.values.sum),
        "__lastEdit" -> VInt64(dates.keys.max.toLong)
      ) ++
      facetTotals
  }

  def makeURI(tileSource: URI, zoom: Int, sk: SpatialKey): URI = {
    val filename = s"${path(zoom, sk)}"
    tileSource.resolve(filename)
  }

  def update(nodes: DataFrame, tileSource: URI, baseZoom: Int = DefaultBaseZoom)(
      implicit concurrentUploads: Option[Int] = None): DataFrame = {
    import nodes.sparkSession.implicits._

    val tiles = if (nodes.columns.contains("facets")) {
      nodes
        .repartition() // eliminate skew
        .as[PointWithKeyAndFacetsAndSequence]
        .tile(baseZoom)
    } else {
      nodes
        .repartition() // eliminate skew
        .as[PointWithKeyAndSequence]
        .tile(baseZoom)
    }

    update(tiles, tileSource, baseZoom)
  }

  def update(
      geometryTiles: Dataset[GeometryTileWithKeyAndSequence],
      tileSource: URI,
      baseZoom: Int)(implicit concurrentUploads: Option[Int], d: DummyImplicit): DataFrame = {
    import geometryTiles.sparkSession.implicits._

    geometryTiles
      .rasterize(BaseCells)
      .pyramid(baseZoom)
      .vectorize
      .groupByKey(tile => (tile.zoom, tile.sk))
      .flatMapGroups {
        case ((zoom, sk), groups: Iterator[VectorTileWithKeyAndSequence]) =>
          groups.toVector.groupBy(_.sequence).map {
            case (sequence, tiles) =>
              VectorTileWithSequence(sequence, zoom, sk, tiles.flatMap(_.features).merge())
          }
      }
      // TODO tiles with different sequences should all be on the same partition
      // creating an S3 tile output stream addresses this, as tiles will have had committed sequences filtered out by
      // the time they hit the output stream
      .mapPartitions { rows: Iterator[VectorTileWithSequence] =>
        // materialize the iterator so that its contents can be used multiple times
        val tiles = rows.toVector

        // increase the number of concurrent network-bound tasks
        implicit val taskSupport: ForkJoinTaskSupport = new ForkJoinTaskSupport(
          new ForkJoinPool(concurrentUploads.getOrElse(DefaultUploadConcurrency)))

        try {
          val urls = makeUrls(tileSource, tiles)
          val mvts = loadMVTs(urls)
          val uncommitted = getUncommittedTiles(tileSource, tiles, mvts)

          updateTiles(tileSource, mvts, uncommitted).iterator
        } finally {
          taskSupport.environment.shutdown()
        }
      }
      .toDF("count", "z", "x", "y")
  }

  def makeUrls(tileSource: URI, tiles: Seq[VectorTileWithSequence]): Map[URI, Extent] =
    tiles.map { tile =>
      (makeURI(tileSource, tile.zoom, tile.sk),
       tile.sk.extent(LayoutScheme.levelForZoom(tile.zoom).layout))
    } toMap

  def getUncommittedTiles(tileSource: URI,
                          tiles: Seq[VectorTileWithSequence],
                          mvts: GenMap[URI, VectorTile]): Iterable[VectorTileWithSequences] =
    tiles
      .groupBy(t => (t.zoom, t.sk))
      .map {
        case ((zoom, sk), rows) =>
          val uri = makeURI(tileSource, zoom, sk)
          val committedSequences =
            mvts.get(uri).map(getCommittedSequences).getOrElse(Set.empty[Int])

          val uncommittedTiles =
            rows.filterNot(t => committedSequences.contains(t.sequence)).toVector

          val sequences = uncommittedTiles.map(_.sequence).toSet

          VectorTileWithSequences(zoom, sk, uncommittedTiles.flatMap(_.features).merge(), sequences)
      }
      .filterNot(x => x.features.isEmpty)

  object implicits {
    implicit class ExtendedPointFeatureTraversableOnce(
        val features: TraversableOnce[PointFeature[Map[String, Long]]]) {

      /**
        * Merge features that are part of the same vectorized tile and contain distinct properties.
        *
        * @return Merged features
        */
      def merge(): Seq[PointFeature[Map[String, Long]]] =
        features
          .foldLeft(Map.empty[Long, PointFeature[Map[String, Long]]]) {
            case (acc, feat) =>
              val sk = feat.data("__id")

              val merged = acc.get(sk) match {
                case Some(f) => f.mapData(d => d ++ feat.data)
                case None    => feat
              }

              acc.updated(sk, merged)
          }
          .values
          .toVector // materialize iterator
    }

    implicit class RasterTileWithKeyExtension(tiles: Dataset[RasterTileWithKey]) {
      import tiles.sparkSession.implicits._

      def vectorize: Dataset[VectorTileWithKey] =
        tiles.map { tile =>
          // convert into features
          val raster = tile.raster
          val rasterExtent =
            RasterExtent(raster.extent, raster.tile.cols, raster.tile.rows)
          val index = new ZSpatialKeyIndex(
            KeyBounds(SpatialKey(0, 0), SpatialKey(raster.tile.cols - 1, raster.tile.rows - 1)))

          val features = ArrayBuffer[PointFeature[Map[String, Long]]]()

          raster.tile.foreach { (c: Int, r: Int, value: Int) =>
            if (value > 0) {
              features.append(
                PointFeature(Point(rasterExtent.gridToMap(c, r)),
                             Map(tile.key -> value,
                                 "__id" -> index.toIndex(SpatialKey(c, r)).toLong)))
            }
          }

          VectorTileWithKey(tile.key, tile.zoom, tile.sk, features)
        }
    }

    implicit class RasterTileWithKeyAndSequenceExtension(
        tiles: Dataset[RasterTileWithKeyAndSequence]) {
      import tiles.sparkSession.implicits._

      def vectorize: Dataset[VectorTileWithKeyAndSequence] =
        tiles.map { tile =>
          // convert into features
          val raster = tile.raster
          val rasterExtent =
            RasterExtent(raster.extent, raster.tile.cols, raster.tile.rows)
          val index = new ZSpatialKeyIndex(
            KeyBounds(SpatialKey(0, 0), SpatialKey(raster.tile.cols - 1, raster.tile.rows - 1)))

          val features = ArrayBuffer[PointFeature[Map[String, Long]]]()

          raster.tile.foreach { (c: Int, r: Int, value: Int) =>
            if (value > 0) {
              features.append(
                PointFeature(Point(rasterExtent.gridToMap(c, r)),
                             Map(tile.key -> value,
                                 "__id" -> index.toIndex(SpatialKey(c, r)).toLong)))
            }
          }

          VectorTileWithKeyAndSequence(tile.sequence, tile.key, tile.zoom, tile.sk, features)
        }
    }
  }
}
