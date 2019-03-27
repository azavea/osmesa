package osmesa.analytics

import java.io.ByteArrayInputStream
import java.net.URI
import java.util.zip.GZIPInputStream

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster._
import geotrellis.raster.resample.Sum
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vector.{Extent, Geometry, PointFeature}
import geotrellis.vectortile.{Layer, VInt64, VectorTile}
import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Dataset
import osmesa.analytics.footprints._
import osmesa.analytics.updater.Implicits._
import osmesa.analytics.updater._
import osmesa.common.raster.MutableSparseIntTile

import scala.collection.GenMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.TaskSupport

trait VectorGrid extends Logging {
  // Default base zoom (highest resolution tiles produced)
  val DefaultBaseZoom: Int = 10

  // Number of columns in a gridded tile
  val Cols: Int = 128
  // Number of rows in a gridded tile
  val Rows: Int = 128

  // Number of columns in a gridded tile at the base of the pyramid (may be used for over-zooming)
  val BaseCols: Int = Cols
  // Number of rows in a gridded tile at the base of the pyramid (may be used for over-zooming)
  val BaseRows: Int = Rows

  // Default upload concurrency
  val DefaultUploadConcurrency: Int = 8

  val LayoutScheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator)
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

  trait implicits {
    implicit class ExtendedCoordinatesWithKey(val coords: Dataset[CoordinatesWithKey]) {
      import coords.sparkSession.implicits._

      def tile(baseZoom: Int = DefaultBaseZoom): Dataset[GeometryTileWithKey] = {
        val layout = LayoutScheme.levelForZoom(baseZoom).layout
        val xs = 0 until layout.layoutCols
        val ys = 0 until layout.layoutRows

        coords
          .flatMap { point =>
            val geom = Geometry(point.geom)

            Option(geom).map(_.reproject(LatLng, WebMercator)) match {
              case Some(g) if g.isValid =>
                layout.mapTransform
                  .keysForGeometry(g)
                  .flatMap { sk =>
                    g.intersection(sk.extent(layout)).toGeometry match {
                      case Some(clipped) if clipped.isValid =>
                        if (xs.contains(sk.col) && ys.contains(sk.row)) {
                          Seq(GeometryTileWithKey(point.key, baseZoom, sk, clipped.jtsGeom))
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

    implicit class ExtendedRasterTileWithKey(val rasterTiles: Dataset[RasterTileWithKey]) {
      import rasterTiles.sparkSession.implicits._

      def pyramid(baseZoom: Int = DefaultBaseZoom): Dataset[RasterTileWithKey] = {
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
          .groupByKey(tile => (tile.key, tile.zoom, tile.sk))
          .mapGroups {
            case ((k, z, sk), tiles: Iterator[RasterTileWithKey]) =>
              val mergedExtent = sk.extent(LayoutScheme.levelForZoom(z).layout)

              val merged = tiles.foldLeft(MutableSparseIntTile(Cols, Rows): Tile)((acc, tile) => {
                if (tile.raster.extent == mergedExtent) {
                  // full resolution raster covering the target extent; no need to merge
                  tile.raster.tile
                } else {
                  acc.merge(mergedExtent, tile.raster.extent, tile.raster.tile, Sum)
                }
              })

              RasterTileWithKey(k, z, sk, (merged, mergedExtent))
          }
      }

      /**
        * Create downsampled versions of input tiles. Will stop when downsampled dimensions drop below 1x1.
        *
        * @param steps Number of steps to pyramid.
        * @param baseZoom Zoom level for the base of the pyramid.
        * @return Base + downsampled tiles.
        */
      def downsample(steps: Int, baseZoom: Int = DefaultBaseZoom): Dataset[RasterTileWithKey] =
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
                                          SpatialKey(tile.sk.col / factor, tile.sk.row / factor),
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

      def pyramid(baseZoom: Int = DefaultBaseZoom): Dataset[RasterTileWithKeyAndSequence] = {
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
      def downsample(steps: Int,
                     baseZoom: Int = DefaultBaseZoom): Dataset[RasterTileWithKeyAndSequence] =
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
                                                 SpatialKey(tile.sk.col / factor,
                                                            tile.sk.row / factor),
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
          .groupByKey(tile => (tile.sequence, tile.key, tile.zoom, tile.sk))
          .mapGroups {
            case ((sequence, k, z, sk), tiles: Iterator[RasterTileWithKeyAndSequence]) =>
              val mergedExtent = sk.extent(LayoutScheme.levelForZoom(z).layout)

              val merged = tiles.foldLeft(MutableSparseIntTile(Cols, Rows): Tile)((acc, tile) => {
                if (tile.raster.extent == mergedExtent) {
                  // full resolution raster covering the target extent; no need to merge
                  tile.raster.tile
                } else {
                  acc.merge(mergedExtent, tile.raster.extent, tile.raster.tile, Sum)
                }
              })

              RasterTileWithKeyAndSequence(sequence, k, z, sk, (merged, mergedExtent))
          }
      }
    }

    implicit class ExtendedGeometryTileWithKey(val geometryTiles: Dataset[GeometryTileWithKey]) {
      def rasterize(cols: Int = Cols, rows: Int = Rows): Dataset[RasterTileWithKey] = {
        import geometryTiles.sparkSession.implicits._

        geometryTiles
          .groupByKey(tile => (tile.key, tile.zoom, tile.sk))
          .mapGroups {
            case ((k, z, sk), tiles) =>
              val tileExtent = sk.extent(LayoutScheme.levelForZoom(z).layout)
              val tile = MutableSparseIntTile(cols, rows)
              val rasterExtent = RasterExtent(tileExtent, tile.cols, tile.rows)
              val geoms = tiles.map(_.geom)

              geoms.foreach(g =>
                Geometry(g).foreach(rasterExtent) { (c, r) =>
                  tile.get(c, r) match {
                    case v if isData(v) => tile.set(c, r, v + 1)
                    case _              => tile.set(c, r, 1)
                  }
              })

              RasterTileWithKey(k, z, sk, (tile, tileExtent))
          }
      }
    }

    implicit class ExtendedGeometryTileWithKeyAndSequence(
        val geometryTiles: Dataset[GeometryTileWithKeyAndSequence]) {
      def rasterize(cols: Int = Cols, rows: Int = Rows): Dataset[RasterTileWithKeyAndSequence] = {
        import geometryTiles.sparkSession.implicits._

        geometryTiles
          .groupByKey(tile => (tile.sequence, tile.key, tile.zoom, tile.sk))
          .mapGroups {
            case ((sequence, k, z, sk), tiles) =>
              val tileExtent = sk.extent(LayoutScheme.levelForZoom(z).layout)
              val tile = MutableSparseIntTile(cols, rows)
              val rasterExtent = RasterExtent(tileExtent, tile.cols, tile.rows)
              val geoms = tiles.map(_.geom)

              geoms.foreach(g =>
                Geometry(g).foreach(rasterExtent) { (c, r) =>
                  tile.get(c, r) match {
                    case v if isData(v) => tile.set(c, r, v + 1)
                    case _              => tile.set(c, r, 1)
                  }
              })

              RasterTileWithKeyAndSequence(sequence, k, z, sk, (tile, tileExtent))
          }
      }
    }

    implicit class ExtendedCoordinatesWithKeyAndSequence(
        val coords: Dataset[CoordinatesWithKeyAndSequence]) {
      import coords.sparkSession.implicits._

      def tile(baseZoom: Int = DefaultBaseZoom): Dataset[GeometryTileWithKeyAndSequence] = {
        val layout = LayoutScheme.levelForZoom(baseZoom).layout

        coords
          .flatMap { point =>
            val geom = point.geom

            Option(geom).map(Geometry(_)).map(_.reproject(LatLng, WebMercator)) match {
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
                                                         sk,
                                                         clipped.jtsGeom))
                      case _ => Seq.empty[GeometryTileWithKeyAndSequence]
                    }
                  }
              case _ => Seq.empty[GeometryTileWithKeyAndSequence]
            }
          }
      }
    }
  }
}
