package osmesa.analytics

import java.io._
import java.net.{URI, URLEncoder}
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPInputStream

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster._
import geotrellis.raster.resample.Sum
import geotrellis.spark.io.index.zcurve.ZSpatialKeyIndex
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.spark.{KeyBounds, SpatialKey}
import geotrellis.vector.{Extent, Feature, Point, PointFeature, Geometry => GTGeometry}
import geotrellis.vectortile.{Layer, VInt64, Value, VectorTile}
import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import osmesa.analytics.footprints._
import osmesa.analytics.updater.Implicits._
import osmesa.analytics.updater.{makeLayer, path, read, write}
import osmesa.common.raster._

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.{ForkJoinTaskSupport, TaskSupport}
import scala.collection.{GenIterable, GenMap}
import scala.concurrent.forkjoin.ForkJoinPool

object Footprints extends Logging {
  val BaseZoom: Int = 14
  val Cols: Int = 512
  val Rows: Int = 512
  val BaseCols: Int = Cols * 4
  val BaseRows: Int = Rows * 4
  val DefaultUploadConcurrency: Int = 8
  val LayoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator)
  val SequenceLayerName: String = "__sequences__"

  import implicits._

  def createFootprints(nodes: DataFrame, tileSource: URI, baseZoom: Int = BaseZoom)(
      implicit concurrentUploads: Option[Int] = None): Dataset[CountWithTileCoordinatesAndKey] = {
    import nodes.sparkSession.implicits._

    val points = nodes
      .repartition() // eliminate skew
      .as[CoordinatesWithKey]

    val pyramid = points.tile(baseZoom).rasterize(BaseCols, BaseRows).pyramid(baseZoom)

    pyramid
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
  }

  def updateFootprints(nodes: DataFrame, tileSource: URI, baseZoom: Int = BaseZoom)(
      implicit concurrentUploads: Option[Int] = None): Dataset[CountWithTileCoordinatesAndKey] = {
    import nodes.sparkSession.implicits._

    val points = nodes
      .repartition() // eliminate skew
      .as[CoordinatesWithKeyAndSequence]

    val pyramid = points.tile(baseZoom).rasterize(BaseCols, BaseRows).pyramid(baseZoom)

    pyramid.groupByKeyAndTile()
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
  }

  def updateTiles(
      tileSource: URI,
      mvts: GenMap[URI, VectorTile],
      tiles: TraversableOnce[
        (String, Int, SpatialKey, Extent, ArrayBuffer[PointFeature[(Long, Int)]], List[Int])])(
      implicit taskSupport: TaskSupport): GenIterable[CountWithTileCoordinatesAndKey] = {
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

        CountWithTileCoordinatesAndKey(feats.size, z, sk.col, sk.row, key)
    }
  }

  def makeFeatures(
      features: Seq[PointFeature[(Long, Int)]]): Seq[Feature[Point, Map[String, VInt64]]] =
    features.map(f =>
      f.mapData {
        case (id, density) =>
          Map("id" -> VInt64(id), "density" -> VInt64(density))
    })

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

  def makeUrls(tileSource: URI,
               tiles: Seq[RasterWithSequenceTileSeqWithTileCoordinatesAndKey]): Map[URI, Extent] =
    tiles.map { tile =>
      val sk = SpatialKey(tile.x, tile.y)

      (makeURI(tileSource, tile.key, tile.zoom, sk),
       sk.extent(LayoutScheme.levelForZoom(tile.zoom).layout))
    } toMap

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

  def getUncommittedTiles(
      tileSource: URI,
      tiles: Seq[RasterWithSequenceTileSeqWithTileCoordinatesAndKey],
      mvts: GenMap[URI, VectorTile]): Seq[RasterWithSequenceTileSeqWithTileCoordinatesAndKey] =
    tiles
      .map(tile => {
        val sk = SpatialKey(tile.x, tile.y)
        val uri = makeURI(tileSource, tile.key, tile.zoom, sk)
        val committedSequences = mvts.get(uri).map(getCommittedSequences).getOrElse(Set.empty[Int])

        RasterWithSequenceTileSeqWithTileCoordinatesAndKey(
          tile.tiles.filterNot(t => committedSequences.contains(t.sequence)),
          tile.zoom,
          tile.x,
          tile.y,
          tile.key)
      })
      .filterNot(tileSeq => tileSeq.tiles.isEmpty)

  def makeURI(tileSource: URI, key: String, zoom: Int, sk: SpatialKey): URI = {
    val filename =
      s"${URLEncoder.encode(key, StandardCharsets.UTF_8.toString)}/${path(zoom, sk)}"
    tileSource.resolve(filename)
  }

  def getCommittedSequences(tile: VectorTile): Set[Int] =
    // NOTE when working with hashtags, this should be the changeset sequence, since changes from a
    // single sequence may appear in different batches depending on when changeset metadata arrives
    tile.layers
      .get(SequenceLayerName)
      .map(_.features.flatMap(f => f.data.values.map(valueToLong).map(_.intValue)))
      .map(_.toSet)
      .getOrElse(Set.empty[Int])

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
            SpatialKey(tile.x, tile.y).extent(LayoutScheme.levelForZoom(tile.zoom).layout)

          val newTile = rasters.foldLeft(MutableSparseIntTile(Cols, Rows): Tile)((acc, raster) =>
            acc.merge(targetExtent, raster.extent, raster.tile, Sum))

          (tile.key, tile.zoom, tile.x, tile.y, (newTile, targetExtent): Raster[Tile], sequences)
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
      .groupByKey(tile => (tile.key, tile.zoom, tile.x, tile.y))
      .mapGroups {
        case ((k, z, x, y), tiles: Iterator[RasterTileWithKeyAndSequence]) =>
          RasterWithSequenceTileSeqWithTileCoordinatesAndKey(
            tiles
              .map(x => RasterWithSequence(x.raster, x.sequence))
              .toVector, // this CANNOT be toSeq, as that produces a stream, which is subsequently partially consumed
            z,
            x,
            y,
            k
          )
      }
  }

  object implicits {
    implicit class ExtendedCoordinatesWithKey(val coords: Dataset[CoordinatesWithKey]) {
      import coords.sparkSession.implicits._

      def tile(baseZoom: Int = BaseZoom): Dataset[GeometryTileWithKey] = {
        val layout = LayoutScheme.levelForZoom(baseZoom).layout

        coords
          .flatMap { point =>
            val geom = point.geom

            Option(geom).map(GTGeometry(_)).map(_.reproject(LatLng, WebMercator)) match {
              case Some(g) if g.isValid =>
                layout.mapTransform
                  .keysForGeometry(g)
                  .flatMap { sk =>
                    g.intersection(sk.extent(layout)).toGeometry match {
                      case Some(clipped) if clipped.isValid =>
                        Seq(
                          GeometryTileWithKey(point.key,
                                              baseZoom,
                                              sk.col,
                                              sk.row,
                                              clipped.jtsGeom))
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
            val geom = point.geom

            Option(geom).map(GTGeometry(_)).map(_.reproject(LatLng, WebMercator)) match {
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

    val getSubPyramid: UserDefinedFunction = udf { (zoom: Int, x: Int, y: Int) =>
      val sp = zoom - (zoom % 4)
      val dz = zoom - sp

      (sp, math.floor(x / math.pow(2, dz)).intValue, math.floor(y / math.pow(2, dz)).intValue)
    }

    implicit class ExtendedRasterTileWithKey(val rasterTiles: Dataset[RasterTileWithKey]) {
      import rasterTiles.sparkSession.implicits._

      def pyramid(baseZoom: Int = BaseZoom): Dataset[RasterTileWithKey] = {
        // Δz between levels of the pyramid; point at which 1 tile becomes 1 pixel
        val dz = (math.log(Cols) / math.log(2)).toInt

        (baseZoom to 0 by -dz)
          .foldLeft(rasterTiles)((acc, z) => acc.downsample(dz, z).merge)
          .repartition('key, getSubPyramid('zoom, 'x, 'y))
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

      def groupByKeyAndTile(): Dataset[RasterWithSequenceTileSeqWithTileCoordinatesAndKey] =
        Footprints.groupByKeyAndTile(rasterTiles)

      def pyramid(baseZoom: Int = BaseZoom): Dataset[RasterTileWithKeyAndSequence] = {
        // Δz between levels of the pyramid; point at which 1 tile becomes 1 pixel
        val dz = (math.log(Cols) / math.log(2)).toInt

        (baseZoom to 0 by -dz)
          .foldLeft(rasterTiles)((acc, z) => acc.downsample(dz, z).merge)
          .repartition('key, getSubPyramid('zoom, 'x, 'y))
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
                        RasterTileWithKeyAndSequence(tile.sequence,
                                                     tile.key,
                                                     zoom,
                                                     tile.x / factor,
                                                     tile.y / factor,
                                                     (parent, raster.extent)))
                    } else {
                      Seq.empty[RasterTileWithKeyAndSequence]
                    }
                  }
              }
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

              RasterTileWithKeyAndSequence(sequence, k, z, x, y, (merged, mergedExtent))
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

              RasterTileWithKeyAndSequence(sequence, k, z, x, y, (tile, tileExtent))
          }
      }
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
          val sk = SpatialKey(tile.x, tile.y)
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

          (tile.key, tile.zoom, sk, raster.extent, features, List.empty[Int])
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
