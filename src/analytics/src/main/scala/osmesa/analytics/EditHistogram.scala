package osmesa.analytics

import java.io._
import java.net.URI
import java.util.zip.GZIPInputStream

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.resample.Sum
import geotrellis.raster.{RasterExtent, _}
import geotrellis.spark.io.index.zcurve.ZSpatialKeyIndex
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.spark.{KeyBounds, SpatialKey}
import geotrellis.vector.{Extent, Feature, Point, PointFeature, Geometry => GTGeometry}
import geotrellis.vectortile.{Layer, VInt64, Value, VectorTile}
import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import osmesa.analytics.footprints._
import osmesa.analytics.updater.Implicits._
import osmesa.analytics.updater.{makeLayer, path, read, write}
import osmesa.common.raster.MutableSparseIntTile

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.{ForkJoinTaskSupport, TaskSupport}
import scala.collection.{GenIterable, GenMap}
import scala.concurrent.forkjoin.ForkJoinPool

object EditHistogram extends Logging {
  val BaseZoom: Int = 10
  val Cols: Int = 128
  val Rows: Int = 128
  val BaseCols: Int = Cols // * 4
  val BaseRows: Int = Rows // * 4
  val DefaultUploadConcurrency: Int = 8
  val LayoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator)
  val SequenceLayerName: String = "__sequences__"

  import implicits._

  def createTiles(nodes: DataFrame, tileSource: URI, baseZoom: Int = BaseZoom)(
      implicit concurrentUploads: Option[Int] = None): DataFrame = {
    import nodes.sparkSession.implicits._

    val points = nodes
      .repartition() // eliminate skew
      .as[CoordinatesWithKey]

    points
      .tile(baseZoom)
      .rasterize(BaseCols, BaseRows)
      .pyramid(baseZoom)
      .vectorize
      .groupByKey(tile => (tile.zoom, tile.x, tile.y))
      .mapGroups {
        case ((zoom, x, y), tiles: Iterator[VectorTileWithKey]) =>
          // use an intermediate Map(spatial key -> feature) to merge partial histograms
          val mergedTiles = tiles
            .flatMap(_.features)
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
            .toVector // materialize iterable

          VectorTileWithSequences(zoom, x, y, mergedTiles, Seq.empty[Int])
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

  def updateTiles(
      tileSource: URI,
      mvts: GenMap[URI, VectorTile],
      tiles: TraversableOnce[VectorTileWithSequences])(
      implicit taskSupport: TaskSupport): GenIterable[(Int, Int, Int, Int)] = {
    // parallelize tiles to facilitate greater upload parallelism
    val parTiles = tiles.toIterable.par
    parTiles.tasksupport = taskSupport

    parTiles.map {
      // update tiles
      case VectorTileWithSequences(z, x, y, feats, sequences) =>
        val sk = SpatialKey(x, y)
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

                val newFeaturesById: Map[Long, Feature[GTGeometry, Map[String, Long]]] =
                  feats
                    .groupBy(_.data("__id"))
                    .mapValues(_.head)
                val featureIds: Set[Long] = newFeaturesById.keySet

                val existingFeatures: Set[Long] =
                  layer.features.map(f => f.data("__id"): Long).toSet

                val unmodifiedFeatures =
                  layer.features.filterNot(f => featureIds.contains(f.data("__id")))

                val modifiedFeatures =
                  layer.features.filter(f => featureIds.contains(f.data("__id")))

                val replacementFeatures: Seq[Feature[GTGeometry, Map[String, Value]]] =
                  modifiedFeatures.map { f =>
                    f.mapData { d =>
                      val merged = d ++ newFeaturesById(d("__id")).data.mapValues(VInt64)
                      val dates = merged.filterKeys(!_.startsWith("__"))

                      merged ++ Map(
                        // sum all edit counts
                        "__total" -> VInt64(dates.values.map(_.toLong).sum),
                        "__lastEdit" -> VInt64(dates.keys.max.toLong)
                      )
                    }
                  }

                val newFeatures =
                  makeFeatures(
                    feats
                      .filterNot(f => existingFeatures.contains(f.data("__id")))).toSeq

                unmodifiedFeatures ++ replacementFeatures ++ newFeatures match {
                  case updatedFeatures if (replacementFeatures.length + newFeatures.length) > 0 =>
                    val updatedLayer = makeLayer(key, extent, updatedFeatures)
                    val sequenceLayer =
                      makeSequenceLayer(getCommittedSequences(tile) ++ sequences, extent)

                    Some(updatedLayer, sequenceLayer)
                  case _ =>
                    logError(s"No changes to $uri; THIS SHOULD NOT HAVE HAPPENED.")
                    None
                }
              case None =>
                Some(makeLayer(key, extent, makeFeatures(feats)),
                     makeSequenceLayer(sequences, extent))
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
            write(VectorTile(Map(makeLayer(key, extent, makeFeatures(feats)),
                                 makeSequenceLayer(sequences, extent)),
                             extent),
                  uri)
        }

        (feats.size, z, sk.col, sk.row)
    }
  }

  def makeFeatures(features: Iterable[PointFeature[Map[String, Long]]])
    : Iterable[Feature[Point, Map[String, VInt64]]] =
    features
      .map(f =>
        f.mapData(d => {
          val dates = d.filterKeys(!_.startsWith("__"))

          // convert values
          d.mapValues(VInt64) ++ Map(
            // sum all edit counts
            "__total" -> VInt64(dates.values.sum),
            "__lastEdit" -> VInt64(dates.keys.max.toLong)
          )
        }))
      .toSeq
      // put recently-edited features first
      .sortBy(_.data.keys.toSeq.max)
      .reverse

  def makeSequenceLayer(sequences: Seq[Int], extent: Extent): (String, Layer) = {
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

  def makeUrls(tileSource: URI, tiles: Seq[VectorTileWithSequence]): Map[URI, Extent] =
    tiles.map { tile =>
      val sk = SpatialKey(tile.x, tile.y)

      (makeURI(tileSource, tile.zoom, sk), sk.extent(LayoutScheme.levelForZoom(tile.zoom).layout))
    } toMap

  def loadMVTs(urls: Map[URI, Extent])(
      implicit taskSupport: TaskSupport): GenMap[URI, VectorTile] = {
    // convert to a parallel collection to load more tiles concurrently
    val parUrls = urls.par
    parUrls.tasksupport = taskSupport

    parUrls.map {
      case (uri, extent) =>
        (uri,
        // TODO gracefully handle errors reading (since failed reads can corrupt the tileset)
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

  def update(nodes: DataFrame, tileSource: URI, baseZoom: Int = BaseZoom)(
      implicit concurrentUploads: Option[Int] = None): DataFrame = {
    import nodes.sparkSession.implicits._

    val points = nodes
      .repartition() // eliminate skew
      .as[CoordinatesWithKeyAndSequence]

     points
      .tile(baseZoom)
      .rasterize(BaseCols, BaseRows)
      .pyramid(baseZoom)
      .vectorize
      .groupByKey(tile => (tile.zoom, tile.x, tile.y))
      .flatMapGroups {
        case ((zoom, x, y), groups: Iterator[VectorTileWithKeyAndSequence]) =>
          groups.toVector.groupBy(_.sequence).map {
            case (sequence, tiles: Vector[VectorTileWithKeyAndSequence]) =>
              {
                val features = tiles
                  .flatMap(_.features)
                  // TODO this occurs in 3 places
                  .foldLeft(Map.empty[Long, PointFeature[Map[String, Long]]]) {
                    // use an intermediate Map(spatial key -> feature) to merge partial histograms
                    case (acc, feat) =>
                      val sk = feat.data("__id")

                      val merged = acc.get(sk) match {
                        case Some(f) => f.mapData(d => d ++ feat.data)
                        case None    => feat
                      }

                      acc.updated(sk, merged)
                  }
                  .values
                  .toVector // materialize iterable

                VectorTileWithSequence(sequence, zoom, x, y, features)
              }
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

  def getUncommittedTiles(tileSource: URI,
                          tiles: Seq[VectorTileWithSequence],
                          mvts: GenMap[URI, VectorTile]): Iterable[VectorTileWithSequences] =
    tiles
      .groupBy(t => (t.zoom, t.x, t.y))
      .map {
        case ((zoom, x, y), rows) => {
          val sk = SpatialKey(x, y)
          val uri = makeURI(tileSource, zoom, sk)
          val committedSequences =
            mvts.get(uri).map(getCommittedSequences).getOrElse(Seq.empty[Int])

          val uncommittedTiles =
            rows.filterNot(t => committedSequences.contains(t.sequence)).toVector

          val sequences = uncommittedTiles.map(_.sequence)
          val features = uncommittedTiles
            .flatMap(_.features)
            .foldLeft(Map.empty[Long, PointFeature[Map[String, Long]]]) {
              // use an intermediate Map(spatial key -> feature) to merge partial histograms
              case (acc, feat) =>
                val sk = feat.data("__id")

                val merged = acc.get(sk) match {
                  case Some(f) => f.mapData(d => d ++ feat.data)
                  case None    => feat
                }

                acc.updated(sk, merged)
            }
            .values
            .toVector

          VectorTileWithSequences(zoom, x, y, features, sequences)
        }
      }
      .filterNot(x => x.features.isEmpty)

  def makeURI(tileSource: URI, zoom: Int, sk: SpatialKey): URI = {
    val filename = s"${path(zoom, sk)}"
    tileSource.resolve(filename)
  }

  def getCommittedSequences(tile: VectorTile): Seq[Int] =
    // NOTE when working with hashtags, this should be the changeset sequence, since changes from a
    // single sequence may appear in different batches depending on when changeset metadata arrives
    tile.layers
      .get(SequenceLayerName)
      .map(_.features.flatMap(f => f.data.values.map(valueToLong).map(_.intValue)))
      .getOrElse(Seq.empty[Int])

  object implicits {
    implicit class ExtendedCoordinatesWithKey(val coords: Dataset[CoordinatesWithKey]) {
      import coords.sparkSession.implicits._

      def tile(baseZoom: Int = BaseZoom): Dataset[GeometryTileWithKey] = {
        val layout = LayoutScheme.levelForZoom(baseZoom).layout
        val xs = 0 until layout.layoutCols
        val ys = 0 until layout.layoutRows

        coords
          .flatMap { point =>
            val geom = GTGeometry(point.geom)

            Option(geom).map(_.reproject(LatLng, WebMercator)) match {
              case Some(g) if g.isValid =>
                layout.mapTransform
                  .keysForGeometry(g)
                  .flatMap { sk =>
                    g.intersection(sk.extent(layout)).toGeometry match {
                      case Some(clipped) if clipped.isValid =>
                        if (xs.contains(sk.col) && ys.contains(sk.row)) {
                          Seq(
                            GeometryTileWithKey(point.key,
                                                baseZoom,
                                                sk.col,
                                                sk.row,
                                                clipped.jtsGeom))
                        } else {
                          // TODO figure out why this happens (0/0/-5)
                          // it might be features extending outside Web Mercator bounds, in which case those features
                          // belong in valid tiles (but need to have their coordinates adjusted to account for that)
                          log.warn(s"Out of range: ${sk.col}, ${sk.row}, ${clipped}")
                          Seq.empty[GeometryTileWithKey]
                        }
                      case _ =>
                        Seq.empty[GeometryTileWithKey]
                    }
                  }
              case _ => Seq.empty[GeometryTileWithKey]
            }
          }
      }
    }

    implicit class ExtendedCoordinatesWithKeyAndSequence(
        val coords: Dataset[CoordinatesWithKeyAndSequence]) {
      import coords.sparkSession.implicits._

      def tile(baseZoom: Int = BaseZoom): Dataset[GeometryTileWithKeyAndSequence] = {
        val layout = LayoutScheme.levelForZoom(baseZoom).layout

        coords
          .flatMap { point =>
            val geom = GTGeometry(point.geom)

            Option(geom).map(_.reproject(LatLng, WebMercator)) match {
              case Some(g) if g.isValid =>
                layout.mapTransform
                  .keysForGeometry(g)
                  .flatMap { sk =>
                    g.intersection(sk.extent(layout)).toGeometry match {
                      case Some(clipped) if clipped.isValid =>
                        Seq(
                          GeometryTileWithKeyAndSequence(point.sequence,
                                                         point.key,
                                                         baseZoom,
                                                         sk.col,
                                                         sk.row,
                                                         clipped.jtsGeom))
                      case _ => Seq.empty[GeometryTileWithKeyAndSequence]
                    }
                  }
              case _ => Seq.empty[GeometryTileWithKeyAndSequence]
            }
          }
      }
    }

    implicit class ExtendedRasterTileWithKey(val rasterTiles: Dataset[RasterTileWithKey]) {
      import rasterTiles.sparkSession.implicits._

      def pyramid(baseZoom: Int = BaseZoom): Dataset[RasterTileWithKey] = {
        // Δz between levels of the pyramid; point at which 1 tile becomes 1 pixel
        val dz = (math.log(Cols) / math.log(2)).toInt

        (baseZoom to 0 by -dz)
          .foldLeft(rasterTiles)((acc, z) => acc.downsample(dz, z).merge)
      }

      /**
        * Merge tiles containing raster subsets.
        *
        * @return Merged tiles.
        */
      def merge: Dataset[RasterTileWithKey] = {
        import rasterTiles.sparkSession.implicits._

        rasterTiles
          .groupByKey(tile => (tile.key, tile.zoom, tile.x, tile.y))
          .mapGroups {
            case ((k, z, x, y), tiles: Iterator[RasterTileWithKey]) =>
              val mergedExtent = SpatialKey(x, y).extent(LayoutScheme.levelForZoom(z).layout)

              val merged = tiles.foldLeft(MutableSparseIntTile(Cols, Rows): Tile)((acc, tile) => {
                if (tile.raster.extent == mergedExtent) {
                  // full resolution raster covering the target extent; no need to merge
                  tile.raster.tile
                } else {
                  acc.merge(mergedExtent, tile.raster.extent, tile.raster.tile, Sum)
                }
              })

              RasterTileWithKey(k, z, x, y, (merged, mergedExtent))
          }
      }

      /**
        * Create downsampled versions of input tiles. Will stop when downsampled dimensions drop below 1x1.
        *
        * @param steps Number of steps to pyramid.
        * @param baseZoom Zoom level for the base of the pyramid.
        * @return Base + downsampled tiles.
        */
      def downsample(steps: Int, baseZoom: Int = BaseZoom): Dataset[RasterTileWithKey] =
        rasterTiles
          .flatMap(tile =>
            if (tile.zoom == baseZoom) {
              val raster = tile.raster
              var parent = raster.tile

              (tile.zoom to math.max(0, tile.zoom - steps) by -1).iterator.flatMap {
                zoom =>
                  if (zoom == tile.zoom) {
                    Seq(tile)
                  } else {
                    val dz = tile.zoom - zoom
                    val factor = math.pow(2, dz).intValue
                    val newCols = Cols / factor
                    val newRows = Rows / factor

                    if (parent.cols > newCols && newCols > 0) {
                      // only resample if the raster is getting smaller
                      parent = parent.resample(newCols, newRows, Sum)

                      Seq(
                        RasterTileWithKey(tile.key,
                                          zoom,
                                          tile.x / factor,
                                          tile.y / factor,
                                          (parent, raster.extent)))
                    } else {
                      Seq.empty[RasterTileWithKey]
                    }
                  }
              }
            } else {
              Seq(tile)
          })
    }

    implicit class ExtendedRasterTileWithKeyAndSequence(
        val rasterTiles: Dataset[RasterTileWithKeyAndSequence]) {
      import rasterTiles.sparkSession.implicits._

      def pyramid(baseZoom: Int = BaseZoom): Dataset[RasterTileWithKeyAndSequence] = {
        // Δz between levels of the pyramid; point at which 1 tile becomes 1 pixel
        val dz = (math.log(Cols) / math.log(2)).toInt

        (baseZoom to 0 by -dz)
          .foldLeft(rasterTiles)((acc, z) => acc.downsample(dz, z).merge)
      }

      /**
        * Create downsampled versions of input tiles. Will stop when downsampled dimensions drop below 1x1.
        *
        * @param steps Number of steps to pyramid.
        * @param baseZoom Zoom level for the base of the pyramid.
        * @return Base + downsampled tiles.
        */
      def downsample(steps: Int, baseZoom: Int = BaseZoom): Dataset[RasterTileWithKeyAndSequence] =
        rasterTiles
          .flatMap(tile =>
            if (tile.zoom == baseZoom) {
              val tiles = ArrayBuffer(tile)

              var parent = tile.raster.tile

              for (zoom <- tile.zoom - 1 to math.max(0, tile.zoom - steps) by -1) {
                val dz = tile.zoom - zoom
                val factor = math.pow(2, dz).intValue
                val newCols = Cols / factor
                val newRows = Rows / factor

                if (parent.cols > newCols && newCols > 0) {
                  // only resample if the raster is getting smaller
                  parent = parent.resample(newCols, newRows, Sum)

                  tiles.append(
                    RasterTileWithKeyAndSequence(tile.sequence,
                                                 tile.key,
                                                 zoom,
                                                 tile.x / factor,
                                                 tile.y / factor,
                                                 (parent, tile.raster.extent)))
                }
              }

              tiles
            } else {
              Seq(tile)
          })

      /**
        * Merge tiles containing raster subsets.
        *
        * @return Merged tiles.
        */
      def merge: Dataset[RasterTileWithKeyAndSequence] = {
        import rasterTiles.sparkSession.implicits._

        rasterTiles
          .groupByKey(tile => (tile.sequence, tile.key, tile.zoom, tile.x, tile.y))
          .mapGroups {
            case ((sequence, k, z, x, y), tiles: Iterator[RasterTileWithKeyAndSequence]) =>
              val mergedExtent = SpatialKey(x, y).extent(LayoutScheme.levelForZoom(z).layout)

              val merged = tiles.foldLeft(MutableSparseIntTile(Cols, Rows): Tile)((acc, tile) => {
                if (tile.raster.extent == mergedExtent) {
                  // full resolution raster covering the target extent; no need to merge
                  tile.raster.tile
                } else {
                  acc.merge(mergedExtent, tile.raster.extent, tile.raster.tile, Sum)
                }
              })

              RasterTileWithKeyAndSequence(sequence,
                                           k,
                                           z,
                                           x,
                                           y,
                                           (merged, mergedExtent))
          }
      }
    }

    implicit class ExtendedGeometryTileWithKey(val geometryTiles: Dataset[GeometryTileWithKey]) {
      def rasterize(cols: Int = Cols, rows: Int = Rows): Dataset[RasterTileWithKey] = {
        import geometryTiles.sparkSession.implicits._

        geometryTiles
          .groupByKey(tile => (tile.key, tile.zoom, tile.x, tile.y))
          .mapGroups {
            case ((k, z, x, y), tiles) =>
              val sk = SpatialKey(x, y)
              val tileExtent = sk.extent(LayoutScheme.levelForZoom(z).layout)
              val tile = MutableSparseIntTile(cols, rows)
              val rasterExtent = RasterExtent(tileExtent, tile.cols, tile.rows)
              val geoms = tiles.map(_.geom)

              geoms.foreach(g =>
                GTGeometry(g).foreach(rasterExtent) { (c, r) =>
                  tile.get(c, r) match {
                    case v if isData(v) => tile.set(c, r, v + 1)
                    case _              => tile.set(c, r, 1)
                  }
              })

              RasterTileWithKey(k, z, x, y, (tile, tileExtent))
          }
      }
    }

    implicit class ExtendedGeometryTileWithKeyAndSequence(
        val geometryTiles: Dataset[GeometryTileWithKeyAndSequence]) {
      def rasterize(cols: Int = Cols, rows: Int = Rows): Dataset[RasterTileWithKeyAndSequence] = {
        import geometryTiles.sparkSession.implicits._

        geometryTiles
          .groupByKey(tile => (tile.sequence, tile.key, tile.zoom, tile.x, tile.y))
          .mapGroups {
            case ((sequence, k, z, x, y), tiles) =>
              val sk = SpatialKey(x, y)
              val tileExtent = sk.extent(LayoutScheme.levelForZoom(z).layout)
              val tile = MutableSparseIntTile(cols, rows)
              val rasterExtent = RasterExtent(tileExtent, tile.cols, tile.rows)
              val geoms = tiles.map(_.geom)

              geoms.foreach(g =>
                GTGeometry(g).foreach(rasterExtent) { (c, r) =>
                  tile.get(c, r) match {
                    case v if isData(v) => tile.set(c, r, v + 1)
                    case _              => tile.set(c, r, 1)
                  }
              })

              RasterTileWithKeyAndSequence(sequence,
                                           k,
                                           z,
                                           x,
                                           y,
                                           (tile, tileExtent))
          }
      }
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

          VectorTileWithKey(tile.key, tile.zoom, tile.x, tile.y, features)
        }
    }

    implicit class RasterTileWithKeyAndSequenceExtension(tiles: Dataset[RasterTileWithKeyAndSequence]) {
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

          VectorTileWithKeyAndSequence(tile.sequence, tile.key, tile.zoom, tile.x, tile.y, features)
        }
    }
  }
}
