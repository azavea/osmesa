package osmesa.analytics.oneoffs

import java.io._
import java.net.{URI, URLEncoder}
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPInputStream

import cats.implicits._
import com.monovore.decline._
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.resample.Sum
import geotrellis.raster.{IntArrayTile, RasterExtent, Raster => GTRaster, _}
import geotrellis.spark.io.index.zcurve.ZSpatialKeyIndex
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.spark.{KeyBounds, SpatialKey}
import geotrellis.vector.io._
import geotrellis.vector.{Extent, Feature, Point, PointFeature, Geometry => GTGeometry}
import geotrellis.vectortile.{Layer, VInt64, Value, VectorTile}
import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import osmesa.analytics.Analytics
import osmesa.analytics.oneoffs.traits._
import osmesa.analytics.updater.Implicits._
import osmesa.analytics.updater.{makeLayer, path, read, write}
import osmesa.common.ProcessOSM
import osmesa.common.model.ElementWithSequence

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.{ForkJoinTaskSupport, TaskSupport}
import scala.collection.{GenMap, GenSeq}
import scala.concurrent.forkjoin.ForkJoinPool

// TODO this effectively replaces Footprint.scala, as it includes both generation + updating logic
object Footprints extends Logging {
  val BaseZoom: Int = 15
  val Cols: Int = 512
  val Rows: Int = 512
  val DefaultUploadConcurrency: Int = 8
  val LayoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator)
  val SequenceLayerName: String = "__sequences__"

  import implicits._

  def pyramid(base: Dataset[RasterTile with Key with Sequence],
              baseZoom: Int = BaseZoom): Dataset[RasterTile with Key with Sequence] =
    (baseZoom to 0 by -8).foldLeft(base)((acc, z) => acc.downsample(z).merge)

  def updateFootprints(tileSource: URI, nodes: DataFrame)(
      implicit concurrentUploads: Option[Int]): Dataset[Count with TileCoordinates with Key] = {
    import nodes.sparkSession.implicits._

    val points = nodes
      .as[CoordinatesWithKeyAndSequence]
      .asInstanceOf[Dataset[Coordinates with Key with Sequence]]

    val pyramid = points.tile(BaseZoom).rasterize(Cols * 4, Rows * 4).pyramid(BaseZoom)

    pyramid.groupByKeyAndTile
      .mapPartitions { rows: Iterator[TileCoordinates with Key with RasterWithSequenceTileSeq] =>
        // materialize the iterator so that its contents can be used multiple times
        val tiles = rows.toList

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
      tiles: Seq[
        (String, Int, SpatialKey, Extent, ArrayBuffer[PointFeature[(Long, Int)]], List[Int])])(
      implicit taskSupport: TaskSupport): GenSeq[Count with TileCoordinates with Key] = {
    // parallelize tiles to facilitate greater upload parallelism
    val parTiles = tiles.par
    parTiles.tasksupport = taskSupport

    parTiles.map {
      // update tiles
      case (key, z, sk, extent, feats, sequences) =>
        val uri = makeURI(tileSource, key, z, sk)

        mvts.get(uri) match {
          case Some(tile) =>
            // update existing tiles

            // load the target layer
            val layer = tile.layers(key)

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
              feats
                .filterNot(f => existingFeatures.contains(f.data._1))
                .map { f =>
                  f.mapData {
                    case (id, density) =>
                      Map("id" -> VInt64(id), "density" -> VInt64(density))
                  }
                }

            unmodifiedFeatures ++ replacementFeatures ++ newFeatures match {
              case updatedFeatures if (replacementFeatures.length + newFeatures.length) > 0 =>
                val updatedLayer = makeLayer(key, extent, updatedFeatures)
                val sequenceLayer =
                  makeSequenceLayer(getCommittedSequences(tile) ++ sequences, extent)

                // merge all available layers into a new tile
                val newTile =
                  VectorTile(
                    tile.layers
                      .updated(updatedLayer._1, updatedLayer._2)
                      // update a second layer with a feature corresponding to committed sequences
                      .updated(sequenceLayer._1, sequenceLayer._2),
                    extent
                  )

                write(newTile, uri)
              case _ =>
                logError(s"No changes to $uri; THIS SHOULD NOT HAVE HAPPENED.")
            }
          case None =>
            // create tile
            val vtFeatures =
              feats.map(f =>
                f.mapData {
                  case (id, density) =>
                    Map("id" -> VInt64(id), "density" -> VInt64(density))
              })

            write(VectorTile(Map(makeLayer(key, extent, vtFeatures),
                                 makeSequenceLayer(sequences, extent)),
                             extent),
                  uri)
        }

        CountWithTileCoordinatesAndKey(feats.size, z, sk.col, sk.row, key)
    }
  }

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

  def makeUrls(tileSource: URI, tiles: Seq[TileCoordinates with Key]): Seq[(URI, Extent)] =
    tiles.map { tile =>
      val sk = SpatialKey(tile.x, tile.y)

      (makeURI(tileSource, tile.key, tile.zoom, sk),
       sk.extent(LayoutScheme.levelForZoom(tile.zoom).layout))
    }

  def loadMVTs(urls: Seq[(URI, Extent)])(
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
    } toMap
  }

  def getUncommittedTiles(
      tileSource: URI,
      tiles: Seq[TileCoordinates with Key with RasterWithSequenceTileSeq],
      mvts: GenMap[URI, VectorTile]): Seq[TileCoordinates with Key with RasterWithSequenceTileSeq] =
    tiles
      .map(tile => {
        val sk = SpatialKey(tile.x, tile.y)
        val uri = makeURI(tileSource, tile.key, tile.zoom, sk)

        RasterWithSequenceTileSeqWithTileCoordinatesAndKey(
          tile.tiles.filter(t =>
            !mvts.get(uri).map(getCommittedSequences).exists(_.contains(t.sequence))),
          tile.zoom,
          tile.x,
          tile.y,
          tile.key)
      })
      .filter(tileSeq => tileSeq.tiles.nonEmpty)

  def makeURI(tileSource: URI, key: String, zoom: Int, sk: SpatialKey): URI = {
    val filename =
      s"${URLEncoder.encode(key, StandardCharsets.UTF_8.toString)}/${path(zoom, sk)}"
    tileSource.resolve(filename)
  }

  def getCommittedSequences(tile: VectorTile): Seq[Int] =
    // NOTE when working with hashtags, this should be the changeset sequence, since changes from a
    // single sequence may appear in different batches depending on when changeset metadata arrives
    tile.layers
      .get(SequenceLayerName)
      .map(_.features.flatMap(f => f.data.values.map(valueToLong).map(_.intValue)))
      .getOrElse(Seq.empty[Int])

  def vectorize(tiles: Seq[(String, Int, Int, Int, GTRaster[Tile], List[Int])])
    : Seq[(String, Int, SpatialKey, Extent, ArrayBuffer[PointFeature[(Long, Int)]], List[Int])] =
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
                           (index.toIndex(SpatialKey(c, r)), value)))
          }
        }

        (k, z, sk, raster.extent, features, sequences)
    }

  def merge(uncommitted: Seq[TileCoordinates with Key with RasterWithSequenceTileSeq])
    : Seq[(String, Int, Int, Int, GTRaster[Tile], List[Int])] =
    uncommitted
      .map {
        // merge tiles with different sequences together
        tile =>
          val data = tile.tiles.toList
          val sequences = data.map(_.sequence)
          val rasters = data.map(_.raster)
          val targetExtent =
            SpatialKey(tile.x, tile.y).extent(LayoutScheme.levelForZoom(tile.zoom).layout)

          val newTile = rasters.head.tile.prototype(Cols, Rows)

          rasters.foreach { raster => newTile.merge(targetExtent, raster.extent, raster.tile, Sum)
          }

          (tile.key, tile.zoom, tile.x, tile.y, GTRaster.tupToRaster(newTile, targetExtent), sequences)
      }

  /**
    * Group tiles by key and tile coordinates.
    *
    * @param tiles Tiles to group.
    * @return Tiles grouped by key and tile coordinates.
    */
  def groupByKeyAndTile(tiles: Dataset[RasterTile with Key with Sequence])
    : Dataset[TileCoordinates with Key with RasterWithSequenceTileSeq] = {
    import tiles.sparkSession.implicits._

    tiles
      .groupByKey(tile => (tile.key, tile.zoom, tile.x, tile.y))
      .mapGroups {
        case ((k, z, x, y), tiles: Iterator[RasterTile with Key with Sequence]) =>
          RasterWithSequenceTileSeqWithTileCoordinatesAndKey(
            tiles
              .map(x => RasterWithSequence(x.raster, x.sequence))
              .toSeq,
            z,
            x,
            y,
            k)
      }
  }

  /**
    * Create downsampled versions of input tiles. Assumes 256x256 tiles and will stop when downsampled dimensions drop
    * below 1x1.
    *
    * @param tiles Tiles to downsample.
    * @param baseZoom Zoom level for the base of the pyramid.
    * @return Base + downsampled tiles.
    */
  def downsample(tiles: Dataset[RasterTile with Key with Sequence],
                 baseZoom: Int = BaseZoom): Dataset[RasterTile with Key with Sequence] =
    tiles
      .flatMap(tile =>
        if (tile.zoom == baseZoom) {
          val tiles = ArrayBuffer(tile)

          var parent = tile.raster.tile

          // with 256x256 tiles, we can't go past <current zoom> - 8, as values sum into partial pixels at that
          // point
          for (zoom <- tile.zoom - 1 to math.max(0, tile.zoom - 8) by -1) {
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
                                             GTRaster.tupToRaster(parent, tile.raster.extent)))
            }
          }

          tiles
        } else {
          Seq(tile)
      })

  /**
    * Merge tiles containing raster subsets.
    *
    * @param tiles Tiles to merge.
    * @return Merged tiles.
    */
  def merge(tiles: Dataset[RasterTile with Key with Sequence])
    : Dataset[RasterTile with Key with Sequence] = {
    import tiles.sparkSession.implicits._

    tiles
      .groupByKey(tile => (tile.sequence, tile.key, tile.zoom, tile.x, tile.y))
      .mapGroups {
        case ((sequence, k, z, x, y), tiles: Iterator[RasterTile with Key with Sequence]) =>
          tiles.map(_.raster).toList match {
            case Seq(raster: GTRaster[Tile]) if raster.cols >= Cols =>
              // single, full-resolution raster (no need to merge)
              RasterTileWithKeyAndSequence(sequence, k, z, x, y, raster)
            case rasters =>
              val targetExtent = SpatialKey(x, y).extent(LayoutScheme.levelForZoom(z).layout)

              val newTile = rasters.head.tile.prototype(Cols, Rows)

              rasters.foreach { raster =>
                newTile.merge(targetExtent, raster.extent, raster.tile, Sum)
              }

              RasterTileWithKeyAndSequence(sequence,
                                           k,
                                           z,
                                           x,
                                           y,
                                           GTRaster.tupToRaster(newTile, targetExtent))
          }
      }
  }

  def rasterize(geometryTiles: Dataset[GeometryTile with Key with Sequence],
                cols: Int = Cols,
                rows: Int = Rows): Dataset[RasterTile with Key with Sequence] = {
    import geometryTiles.sparkSession.implicits._

    geometryTiles
      .groupByKey(tile => (tile.sequence, tile.key, tile.zoom, tile.x, tile.y))
      .mapGroups {
        case ((sequence, k, z, x, y), rows) =>
          val tiles = rows.toList
          val sk = SpatialKey(x, y)
          val tileExtent = sk.extent(LayoutScheme.levelForZoom(z).layout)
          val tile = IntArrayTile.ofDim(Cols * 4, Rows * 4, IntCellType)
          val rasterExtent = RasterExtent(tileExtent, tile.cols, tile.rows)
          val geoms = tiles.map(_.wkb.readWKB)

          geoms.foreach(g =>
            g.foreach(rasterExtent) { (c, r) => tile.set(c, r, tile.get(c, r) + 1)
          })

          RasterTileWithKeyAndSequence(sequence, k, z, x, y, GTRaster.tupToRaster(tile, tileExtent))
      }
  }

  def tile(coords: Dataset[Coordinates with Key with Sequence],
           baseZoom: Int = BaseZoom): Dataset[GeometryTile with Key with Sequence] = {
    val layout = LayoutScheme.levelForZoom(baseZoom).layout

    coords
      .flatMap { point =>
        val geom = point.geom

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
                                                     clipped.toWKB(3857)))
                  case _ => Seq.empty[GeometryTileWithKeyAndSequence]
                }
              }
          case _ => Seq.empty[GeometryTileWithKeyAndSequence]
        }
      }
  }

  object implicits {
    implicit val CountWithTileCoordinatesAndKeyEncoder
      : Encoder[Count with TileCoordinates with Key] = Encoders
      .product[CountWithTileCoordinatesAndKey]
      .asInstanceOf[Encoder[Count with TileCoordinates with Key]]

    implicit val GeometryTileWithKeyAndSequenceEncoder
      : Encoder[GeometryTile with Key with Sequence] =
      Encoders
        .product[GeometryTileWithKeyAndSequence]
        .asInstanceOf[Encoder[GeometryTile with Key with Sequence]]

    implicit val RasterTileWithKeyAndSequenceEncoder: Encoder[RasterTile with Key with Sequence] =
      Encoders
        .product[RasterTileWithKeyAndSequence]
        .asInstanceOf[Encoder[RasterTile with Key with Sequence]]

    implicit val RasterWithSequenceTileSeqWithTileCoordinatesAndKeyEncoder
      : Encoder[TileCoordinates with Key with RasterWithSequenceTileSeq] = Encoders
      .product[RasterWithSequenceTileSeqWithTileCoordinatesAndKey]
      .asInstanceOf[Encoder[TileCoordinates with Key with RasterWithSequenceTileSeq]]

    implicit class ExtendedCoordinatesWithKeyAndSequence(
        val coords: Dataset[Coordinates with Key with Sequence]) {
      def tile(baseZoom: Int = BaseZoom): Dataset[GeometryTile with Key with Sequence] =
        Footprints.tile(coords, baseZoom)
    }

    implicit class ExtendedRasterTileWithKeyAndSequence(
        val rasterTiles: Dataset[RasterTile with Key with Sequence]) {
      def downsample(baseZoom: Int = BaseZoom): Dataset[RasterTile with Key with Sequence] =
        Footprints.downsample(rasterTiles, baseZoom)

      def groupByKeyAndTile: Dataset[TileCoordinates with Key with RasterWithSequenceTileSeq] =
        Footprints.groupByKeyAndTile(rasterTiles)

      def merge: Dataset[RasterTile with Key with Sequence] = Footprints.merge(rasterTiles)

      def pyramid(baseZoom: Int = BaseZoom): Dataset[RasterTile with Key with Sequence] =
        Footprints.pyramid(rasterTiles, baseZoom)
    }

    implicit class ExtendedGeometryTileWithKeyAndSequence(
        val geometryTiles: Dataset[GeometryTile with Key with Sequence]) {
      def rasterize(cols: Int = Cols,
                    rows: Int = Rows): Dataset[RasterTile with Key with Sequence] =
        Footprints.rasterize(geometryTiles, cols, rows)
    }

    implicit class ExtendedTileCoordinatesWithKeyAndRasterWithSequenceTileSeq(
        uncommitted: Seq[TileCoordinates with Key with RasterWithSequenceTileSeq]) {
      def merge: Seq[(String, Int, Int, Int, GTRaster[Tile], List[Int])] =
        Footprints.merge(uncommitted)
    }

    implicit class VectorizationExtension(
        tiles: Seq[(String, Int, Int, Int, GTRaster[Tile], List[Int])]) {
      def vectorize: Seq[
        (String, Int, SpatialKey, Extent, ArrayBuffer[PointFeature[(Long, Int)]], List[Int])] =
        Footprints.vectorize(tiles)
    }
  }
}

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.UserFootprintUpdater \
 *   ingest/target/scala-2.11/osmesa-analytics.jar
 */
object UserFootprintUpdater
    extends CommandApp(
      name = "osmesa-user-footprint-updater",
      header = "Consume minutely diffs + changesets and update user footprint MVTs",
      main = {
        type AugmentedDiffFeature = Feature[GTGeometry, ElementWithSequence]
        val rootURI = new File("").toURI

        val changeSourceOpt = Opts
          .option[URI]("change-source",
                       short = "d",
                       metavar = "uri",
                       help = "Location of minutely diffs to process")
          .withDefault(new URI("https://planet.osm.org/replication/minute/"))
        val changesStartSequenceOpt = Opts
          .option[Int](
            "changes-start-sequence",
            short = "s",
            metavar = "sequence",
            help =
              "Minutely diff starting sequence. If absent, the current (remote) sequence will be used.")
          .orNone
        val changesEndSequenceOpt = Opts
          .option[Int](
            "changes-end-sequence",
            short = "e",
            metavar = "sequence",
            help = "Minutely diff ending sequence. If absent, this will be an infinite stream.")
          .orNone
        val changesBatchSizeOpt = Opts
          .option[Int]("changes-batch-size",
                       short = "b",
                       metavar = "batch size",
                       help = "Change batch size.")
          .orNone
        val tileSourceOpt = Opts
          .option[URI](
            "tile-source",
            short = "t",
            metavar = "uri",
            help = "URI prefix for vector tiles to update"
          )
          .withDefault(rootURI)
        val concurrentUploadsOpt = Opts
          .option[Int]("concurrent-uploads",
                       short = "c",
                       metavar = "concurrent uploads",
                       help = "Set the number of concurrent uploads.")
          .orNone

        (changeSourceOpt,
         changesStartSequenceOpt,
         changesEndSequenceOpt,
         changesBatchSizeOpt,
         tileSourceOpt,
         concurrentUploadsOpt).mapN {
          (changeSource,
           changesStartSequence,
           changesEndSequence,
           changesBatchSize,
           tileSource,
           _concurrentUploads) =>
            val spark: SparkSession = Analytics.sparkSession("UserFootprintUpdater")
            import spark.implicits._
            implicit val concurrentUploads: Option[Int] = _concurrentUploads

            val changeOptions = Map("base_uri" -> changeSource.toString) ++
              changesStartSequence
                .map(s => Map("start_sequence" -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              changesEndSequence
                .map(s => Map("end_sequence" -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              changesBatchSize
                .map(s => Map("batch_size" -> s.toString))
                .getOrElse(Map.empty[String, String])

            val changes = spark.readStream
              .format("changes")
              .options(changeOptions)
              .load

            val changedNodes = changes
              .where('_type === ProcessOSM.NodeType and 'lat.isNotNull and 'lon.isNotNull)
              .select('sequence, 'user, 'lat, 'lon)

            val tiledNodes =
              Footprints.updateFootprints(tileSource,
                                          changedNodes
                                            .withColumnRenamed("user", "key"))

            val query = tiledNodes.writeStream
              .queryName("tiled user footprints")
              .format("console")
              .start

            query.awaitTermination()

            spark.stop()
        }
      }
    )
