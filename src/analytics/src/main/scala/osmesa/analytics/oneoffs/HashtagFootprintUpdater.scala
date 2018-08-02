package osmesa.analytics.oneoffs

import java.io._
import java.math.BigDecimal
import java.net.{URI, URLEncoder}
import java.nio.charset.StandardCharsets
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import cats.implicits._
import com.monovore.decline._
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.resample.Sum
import geotrellis.raster.{IntArrayTile, Raster, RasterExtent, _}
import geotrellis.spark.io.index.zcurve.ZSpatialKeyIndex
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.spark.{KeyBounds, SpatialKey}
import geotrellis.vector.io._
import geotrellis.vector.{Feature, Geometry, Line, MultiLine, MultiPoint, MultiPolygon, Point, PointFeature, Polygon}
import geotrellis.vectortile.{StrictLayer, VInt64, Value, VectorTile}
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import osmesa.analytics.Analytics
import osmesa.analytics.updater.Implicits._
import osmesa.analytics.updater.{makeLayer, path, read, write}
import osmesa.common.functions.osm._
import osmesa.common.ProcessOSM
import osmesa.common.model.ElementWithSequence

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.HashtagFootprintUpdater \
 *   ingest/target/scala-2.11/osmesa-analytics.jar
 */
object HashtagFootprintUpdater
    extends CommandApp(
      name = "osmesa-hashtag-footprint-updater",
      header = "Consume minutely diffs + changesets and update hashtag footprint MVTs",
      main = {
        type AugmentedDiffFeature = Feature[Geometry, ElementWithSequence]
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
        val changesetSourceOpt =
          Opts
            .option[URI]("changeset-source",
                         short = "c",
                         metavar = "uri",
                         help = "Location of changesets to process")
            .withDefault(new URI("https://planet.osm.org/replication/changesets/"))
        val changesetsStartSequenceOpt = Opts
          .option[Int](
            "changeset-start-sequence",
            short = "S",
            metavar = "sequence",
            help =
              "Changeset starting sequence. If absent, the current (remote) sequence will be used.")
          .orNone
        val changesetsEndSequenceOpt = Opts
          .option[Int]("changeset-end-sequence",
                       short = "E",
                       metavar = "sequence",
                       help =
                         "Changeset ending sequence. If absent, this will be an infinite stream.")
          .orNone
        val changesetsBatchSizeOpt = Opts
          .option[Int]("changesets-batch-size",
                       short = "B",
                       metavar = "batch size",
                       help = "Changeset batch size.")
          .orNone
        val tileSourceOpt = Opts
          .option[URI](
            "tile-source",
            short = "t",
            metavar = "uri",
            help = "URI prefix for vector tiles to update"
          )
          .withDefault(rootURI)

        (changeSourceOpt,
         changesStartSequenceOpt,
         changesEndSequenceOpt,
         changesBatchSizeOpt,
         changesetSourceOpt,
         changesetsStartSequenceOpt,
         changesetsEndSequenceOpt,
         changesetsBatchSizeOpt,
         tileSourceOpt).mapN {
          (changeSource,
           changesStartSequence,
           changesEndSequence,
           changesBatchSize,
           changesetSource,
           changesetsStartSequence,
           changesetsEndSequence,
           changesetsBatchSize,
           tileSource) =>
            implicit val spark: SparkSession = Analytics.sparkSession("HashtagFootprintUpdater")
            import spark.implicits._

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

            val changesetOptions = Map("base_uri" -> changesetSource.toString) ++
              changesetsStartSequence
                .map(s => Map("start_sequence" -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              changesetsEndSequence
                .map(s => Map("end_sequence" -> s.toString))
                .getOrElse(Map.empty[String, String])
            changesetsBatchSize
              .map(s => Map("batch_size" -> s.toString))
              .getOrElse(Map.empty[String, String])

            val changesets = spark.readStream
              .format("changesets")
              .options(changesetOptions)
              .load

            val watermarkedChanges = changes
            // geoms may appear before the changeset they're within or the changeset metadata may have been missed, in
            // which case watch for it to be closed within the next 24 hours
              .withWatermark("timestamp", "25 hours")
              .where('_type === ProcessOSM.NodeType and 'lat.isNotNull and 'lon.isNotNull)
              .select('timestamp, 'changeset, 'lat, 'lon)

            val watermarkedChangesets = changesets
            // changesets can remain open for 24 hours; buy some extra time
            // TODO can projecting into the future (created_at + 24 hours) and coalescing closed_at reduce the number
            // of changesets being tracked?
              .withWatermark("created_at", "25 hours")
              .withColumn("hashtag", explode(hashtags('tags)))
              .select('id as 'changeset, 'hashtag)

            val BASE_ZOOM = 15
            val Cols = 512
            val Rows = 512

            val zoom = BASE_ZOOM
//            val LayoutScheme = ZoomedLayoutScheme(WebMercator)
//            val layout = LayoutScheme.levelForZoom(zoom).layout
            val TiledGeometrySchema = StructType(
              StructField("key", StringType, nullable = false) ::
                StructField("zoom", IntegerType, nullable = false) ::
                StructField("col", IntegerType, nullable = false) ::
                StructField("row", IntegerType, nullable = false) ::
                StructField("geom", BinaryType, nullable = true) ::
                Nil)

            val TiledGeometryEncoder: Encoder[Row] = RowEncoder(TiledGeometrySchema)
            type KeyedTile = (String, Int, Int, Int, Raster[Tile])
            implicit def encodeTile(tile: Tile): (Array[Byte], Int, Int, CellType) =
              (tile.toBytes, tile.cols, tile.rows, tile.cellType)
            implicit def decodeTile(tile: (Array[Byte], Int, Int, CellType)): Tile =
              IntArrayTile.fromBytes(tile._1,
                                     tile._2,
                                     tile._3,
                                     tile._4.asInstanceOf[IntCells with NoDataHandling])

            implicit val tupleEncoder: Encoder[KeyedTile] = Encoders.kryo[KeyedTile]

            val baseQuery = watermarkedChanges
              .join(watermarkedChangesets, Seq("changeset"))

            val tiledHashtags = baseQuery
              .withColumnRenamed("hashtag", "key")
              .flatMap {
                row =>
                  val key = row.getAs[String]("key")
                  val lat = row.getAs[BigDecimal]("lat").doubleValue
                  val lon = row.getAs[BigDecimal]("lon").doubleValue
                  val geom = Point(lon, lat)
                  val LayoutScheme = ZoomedLayoutScheme(WebMercator)
                  val layout = LayoutScheme.levelForZoom(zoom).layout

                  Option(geom).map(_.reproject(LatLng, WebMercator)) match {
                    case Some(g) if g.isValid =>
                      layout.mapTransform
                        .keysForGeometry(g)
                        .flatMap { sk =>
                          g.intersection(sk.extent(layout)).toGeometry match {
                            case Some(clipped) if clipped.isValid =>
                              Seq(new GenericRowWithSchema(
                                Array(key, zoom, sk.col, sk.row, clipped.toWKB(3857)),
                                TiledGeometrySchema): Row)
                            case _ => Seq.empty[Row]
                          }
                        }
                    case _ => Seq.empty[Row]
                  }
              }(TiledGeometryEncoder) groupByKey { row =>
              (row.getAs[String]("key"),
               row.getAs[Int]("zoom"),
               row.getAs[Int]("col"),
               row.getAs[Int]("row"))
            } mapGroups {
              case ((k, z, x, y), rows) =>
                val sk = SpatialKey(x, y)
                val LayoutScheme = ZoomedLayoutScheme(WebMercator)
                val tileExtent = sk.extent(LayoutScheme.levelForZoom(z).layout)
                val tile = IntArrayTile.ofDim(Cols * 4, Rows * 4, IntCellType)
                val rasterExtent = RasterExtent(tileExtent, tile.cols, tile.rows)
                val geoms = rows.map(_.getAs[Array[Byte]]("geom").readWKB)

                geoms.foreach(g =>
                  g.foreach(rasterExtent) { (c, r) =>
                    tile.set(c, r, tile.get(c, r) + 1)
                })

                (k, z, x, y, Raster.tupToRaster(tile, tileExtent))
            } flatMap {
              case (k, z, x, y, raster) =>
                if (z == BASE_ZOOM) {
                  val tiles = ArrayBuffer((k, z, x, y, raster))

                  var parent = raster.tile

                  // with 256x256 tiles, we can't go past <current zoom> - 8, as values sum into partial pixels at that
                  // point
                  for (zoom <- z - 1 to math.max(0, z - 8) by -1) {
                    val dz = z - zoom
                    val factor = math.pow(2, dz).intValue
                    val newCols = Cols / factor
                    val newRows = Rows / factor

                    if (parent.cols > newCols && newCols > 0) {
                      // only resample if the raster is getting smaller
                      parent = parent.resample(newCols, newRows, Sum)
                    }

                    tiles.append(
                      (k, zoom, x / factor, y / factor, Raster.tupToRaster(parent, raster.extent)))
                  }

                  tiles
                } else {
                  Seq((k, z, x, y, raster))
                }
            } groupByKey {
              case (k, z, x, y, _) => (k, z, x, y)
            } mapGroups {
              case ((k, z, x, y), tiles) =>
                tiles.map(_._5).toList match {
                  case Seq(raster: Raster[Tile]) if raster.cols >= Cols =>
                    // single, full-resolution raster (no need to merge)
                    (k, z, x, y, raster)
                  case rasters =>
                    val LayoutScheme = ZoomedLayoutScheme(WebMercator)
                    val targetExtent = SpatialKey(x, y).extent(LayoutScheme.levelForZoom(z).layout)

                    val newTile = rasters.head.tile.prototype(Cols, Rows)

                    rasters.foreach { raster =>
                      newTile.merge(targetExtent, raster.extent, raster.tile, Sum)
                    }

                    (k, z, x, y, Raster.tupToRaster(newTile, targetExtent))
                }
            } flatMap {
              case (k, z, x, y, raster) =>
                if (z == BASE_ZOOM - 8) {
                  // resample z7 tiles to produce lower-zooms
                  val tiles = ArrayBuffer((k, z, x, y, raster))

                  var parent = raster.tile

                  // with 256x256 tiles, we can't go past <current zoom> - 8, as values sum into partial pixels at that
                  // point
                  for (zoom <- z - 1 to math.max(0, z - 8) by -1) {
                    val dz = z - zoom
                    val factor = math.pow(2, dz).intValue
                    val newCols = Cols / factor
                    val newRows = Rows / factor

                    if (parent.cols > newCols && newCols > 0) {
                      // only resample if the raster is getting smaller
                      parent = parent.resample(newCols, newRows, Sum)
                    }

                    tiles.append(
                      (k, zoom, x / factor, y / factor, Raster.tupToRaster(parent, raster.extent)))
                  }

                  tiles
                } else {
                  Seq((k, z, x, y, raster))
                }
            } groupByKey {
              case (k, z, x, y, _) => (k, z, x, y)
            } mapGroups {
              case ((k, z, x, y), tiles) =>
                tiles.map(_._5).toList match {
                  case Seq(raster: Raster[Tile]) if raster.cols >= Cols =>
                    // single, full-resolution raster (no need to merge)
                    (k, z, x, y, raster)
                  case rasters =>
                    val LayoutScheme = ZoomedLayoutScheme(WebMercator)
                    val targetExtent = SpatialKey(x, y).extent(LayoutScheme.levelForZoom(z).layout)

                    val newTile = rasters.head.tile.prototype(Cols, Rows)

                    rasters.foreach { raster =>
                      newTile.merge(targetExtent, raster.extent, raster.tile, Sum)
                    }

                    (k, z, x, y, Raster.tupToRaster(newTile, targetExtent))
                }
            } mapPartitions {
              rows =>
                val features = rows.map {
                  case (k, zoom, x, y, raster) =>
                    val sk = SpatialKey(x, y)
                    val rasterExtent =
                      RasterExtent(raster.extent, raster.tile.cols, raster.tile.rows)
                    val index = new ZSpatialKeyIndex(
                      KeyBounds(SpatialKey(0, 0),
                                SpatialKey(raster.tile.cols - 1, raster.tile.rows - 1)))

                    val features = ArrayBuffer[PointFeature[(Long, Int)]]()

                    raster.tile.foreach { (c: Int, r: Int, value: Int) =>
                      if (value > 0) {
                        features.append(PointFeature(Point(rasterExtent.gridToMap(c, r)),
                                                     (index.toIndex(SpatialKey(c, r)), value)))
                      }
                    }

                    (k, zoom, sk, raster.extent, features)
                }

                val parFeatures = features.toTraversable.par
                // TODO allow size of pool to be increased as "concurrent-uploads-per-executor" or something
                val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(8))

                // increase the number of concurrent uploads
                parFeatures.tasksupport = taskSupport

                val layerName = "hashtag_footprint"

                val modifiedTiles = parFeatures.map {
                  case (key, zoom, sk, extent, feats) =>
                    val filename = s"${URLEncoder.encode(key, StandardCharsets.UTF_8.toString)}/${path(zoom, sk)}"
                    val uri = tileSource.resolve(filename)

                    read(uri) match {
                      case Some(bytes) =>
                        // update existing tiles
                        // NOTE the tiles are unaware of sequence numbers, so encountering the same diffs will
                        // increment values where they should be skipped
                        val tile = VectorTile.fromBytes(
                          IOUtils.toByteArray(new GZIPInputStream(new ByteArrayInputStream(bytes))),
                          extent)

                        // load the target layer
                        val layer = tile.layers(layerName)

                        // TODO check a secondary layer to see whether the current sequence has already been applied
                        // NOTE when working with hashtags, this should be the changeset sequence, since changes from a
                        // single sequence may appear in different batches depending on when changeset metadata arrives

                        val newFeaturesById: Map[Long, Feature[Geometry, (Long, Int)]] =
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

                        val replacementFeatures: Seq[Feature[Geometry, Map[String, Value]]] =
                          modifiedFeatures.map { f =>
                            f.mapData { d =>
                              val prevDensity: Long = d("density")
                              d.updated("density",
                                        VInt64(prevDensity + newFeaturesById(d("id")).data._2))
                            }
                          }

                        val newFeatures: Seq[Feature[Geometry, Map[String, Value]]] =
                          feats
                            .filterNot(f => existingFeatures.contains(f.data._1))
                            .map { f =>
                              f.mapData {
                                case ((id, density)) =>
                                  Map("id" -> VInt64(id), "density" -> VInt64(density))
                              }
                            }

                        unmodifiedFeatures ++ replacementFeatures ++ newFeatures match {
                          case updatedFeatures
                              if (replacementFeatures.length + newFeatures.length) > 0 =>
                            val updatedLayer = makeLayer(layerName, extent, updatedFeatures)

                            // merge all available layers into a new tile
                            // TODO update a second layer w/ features corresponding to sequences seen (in the absence of
                            // tile / layer metadata)
                            val newTile =
                              VectorTile(tile.layers.updated(layerName, updatedLayer), extent)

                            val byteStream = new ByteArrayOutputStream()

                            try {
                              val gzipStream = new GZIPOutputStream(byteStream)
                              try {
                                gzipStream.write(newTile.toBytes)
                              } finally {
                                gzipStream.close()
                              }
                            } finally {
                              byteStream.close()
                            }

                            write(uri, byteStream.toByteArray, Some("gzip"))
                          case _ =>
                            println(s"No changes to $uri; THIS SHOULD NOT HAVE HAPPENED.")
                        }
                      case None =>
                        // create tile
                        val vtFeatures =
                          feats.map(f =>
                            f.mapData {
                              case ((id, density)) =>
                                Map("id" -> VInt64(id), "density" -> VInt64(density))
                          })

                        val layer = StrictLayer(
                          // TODO use key as the layer name
                          name = layerName,
                          tileWidth = 4096,
                          version = 2,
                          tileExtent = extent,
                          points = vtFeatures,
                          multiPoints = Seq.empty[Feature[MultiPoint, Map[String, Value]]],
                          lines = Seq.empty[Feature[Line, Map[String, Value]]],
                          multiLines = Seq.empty[Feature[MultiLine, Map[String, Value]]],
                          polygons = Seq.empty[Feature[Polygon, Map[String, Value]]],
                          multiPolygons = Seq.empty[Feature[MultiPolygon, Map[String, Value]]]
                        )

                        // TODO use key as the layer name
                        // TODO create a second layer w/ features corresponding to sequences seen (in the absence of
                        // tile / layer metadata)
                        val vt = VectorTile(Map(layerName -> layer), extent)

                        val byteStream = new ByteArrayOutputStream()

                        try {
                          val gzipStream = new GZIPOutputStream(byteStream)
                          try {
                            gzipStream.write(vt.toBytes)
                          } finally {
                            gzipStream.close()
                          }
                        } finally {
                          byteStream.close()
                        }

                        write(uri, byteStream.toByteArray, Some("gzip"))
                    }

                    (key, zoom, sk.col, sk.row, feats.size)
                }

                taskSupport.environment.shutdown()

                modifiedTiles.iterator
            }

            val query = tiledHashtags
              .withColumnRenamed("_1", "key")
              .withColumnRenamed("_2", "zoom")
              .withColumnRenamed("_3", "x")
              .withColumnRenamed("_4", "y")
              .withColumnRenamed("_5", "featureCount")
              .writeStream
              .queryName("tiled hashtag footprints")
              .format("console")
              .start

            query.awaitTermination()

            spark.stop()
        }
      }
    )
