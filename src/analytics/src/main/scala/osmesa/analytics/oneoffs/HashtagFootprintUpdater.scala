package osmesa.analytics.oneoffs

import java.io._
import java.net.URI
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
import geotrellis.vector.io.json.JsonFeatureCollectionMap
import geotrellis.vector.{
  Feature,
  Geometry,
  Line,
  MultiLine,
  MultiPoint,
  MultiPolygon,
  Point,
  PointFeature,
  Polygon
}
import geotrellis.vectortile.{StrictLayer, VInt64, Value, VectorTile}
import org.apache.commons.io.IOUtils
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import osmesa.analytics.updater.Implicits._
import osmesa.analytics.updater.{makeLayer, path, read, write}
import osmesa.common.functions.osm._
import osmesa.common.{AugmentedDiff, ProcessOSM}

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
 *   ingest/target/scala-2.11/osmesa-analytics.jar \
 *   --augmented-diff-source s3://somewhere/diffs/ \
 */
object HashtagFootprintUpdater
    extends CommandApp(
      name = "osmesa-hashtag-footprint-updater",
      header = "Consume minutely diffs + changesets and update hashtag footprint MVTs",
      main = {
        type AugmentedDiffFeature = Feature[Geometry, AugmentedDiff]
        val rootURI = new File("").toURI

        val augmentedDiffSourceOpt = Opts.option[URI]("augmented-diff-source",
                                                      short = "a",
                                                      metavar = "uri",
                                                      help =
                                                        "Location of augmented diffs to process")
        val changesetSourceOpt =
          Opts
            .option[URI]("changeset-source",
                         short = "c",
                         metavar = "uri",
                         help = "Location of changesets to process")
            .withDefault(new URI("https://planet.osm.org/replication/changesets/"))
        val startSequenceOpt = Opts
          .option[Int](
            "start-sequence",
            short = "s",
            metavar = "sequence",
            help = "Starting sequence. If absent, the current (remote) sequence will be used.")
          .orNone
        val endSequenceOpt = Opts
          .option[Int]("end-sequence",
                       short = "e",
                       metavar = "sequence",
                       help = "Ending sequence. If absent, this will be an infinite stream.")
          .orNone
        val tileSourceOpt = Opts
          .option[URI](
            "tile-source",
            short = "t",
            metavar = "uri",
            help = "URI prefix for vector tiles to update"
          )
          .withDefault(rootURI)

        (augmentedDiffSourceOpt,
         changesetSourceOpt,
         startSequenceOpt,
         endSequenceOpt,
         tileSourceOpt).mapN {
          (augmentedDiffSource, changesetSource, startSequence, endSequence, tileSource) =>
            /* Settings compatible with both local and EMR execution */
            val conf = new SparkConf()
              .setIfMissing("spark.master", "local[*]")
              .setAppName("merged-changeset-stream-processor")
              .set("spark.serializer", classOf[org.apache.spark.serializer.KryoSerializer].getName)
              .set("spark.kryo.registrator",
                   classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)

            implicit val ss: SparkSession = SparkSession.builder
              .config(conf)
              .enableHiveSupport
              .getOrCreate

            import ss.implicits._

            // read augmented diffs as text for better geometry support (by reading from GeoJSON w/ GeoTrellis)
            val diffs =
              ss.readStream.textFile(augmentedDiffSource.toString)

            implicit val augmentedDiffFeatureEncoder
              : Encoder[(Option[AugmentedDiffFeature], AugmentedDiffFeature)] =
              Encoders.kryo[(Option[AugmentedDiffFeature], AugmentedDiffFeature)]

            val AugmentedDiffSchema = StructType(
              StructField("sequence", LongType) ::
                StructField("_type", ByteType, nullable = false) ::
                StructField("id", LongType, nullable = false) ::
                StructField("prevGeom", BinaryType, nullable = true) ::
                StructField("geom", BinaryType, nullable = true) ::
                StructField("prevTags",
                            MapType(StringType, StringType, valueContainsNull = false),
                            nullable = true) ::
                StructField("tags",
                            MapType(StringType, StringType, valueContainsNull = false),
                            nullable = false) ::
                StructField("prevChangeset", LongType, nullable = true) ::
                StructField("changeset", LongType, nullable = false) ::
                StructField("prevUid", LongType, nullable = true) ::
                StructField("uid", LongType, nullable = false) ::
                StructField("prevUser", StringType, nullable = true) ::
                StructField("user", StringType, nullable = false) ::
                StructField("prevUpdated", TimestampType, nullable = true) ::
                StructField("updated", TimestampType, nullable = false) ::
                StructField("prevVisible", BooleanType, nullable = true) ::
                StructField("visible", BooleanType, nullable = false) ::
                StructField("prevVersion", IntegerType, nullable = true) ::
                StructField("version", IntegerType, nullable = false) ::
                StructField("prevMinorVersion", IntegerType, nullable = true) ::
                StructField("minorVersion", IntegerType, nullable = false) ::
                Nil)

            val AugmentedDiffEncoder: Encoder[Row] = RowEncoder(AugmentedDiffSchema)

            implicit val encoder: Encoder[Row] = AugmentedDiffEncoder

            val geoms = diffs map { line =>
              val features = line
              // Spark doesn't like RS-delimited JSON; perhaps Spray doesn't either
                .replace("\u001e", "")
                .parseGeoJson[JsonFeatureCollectionMap]
                .getAll[AugmentedDiffFeature]

              (features.get("old"), features("new"))
            } map {
              case (Some(prev), curr) =>
                val _type = curr.data.elementType match {
                  case "node"     => ProcessOSM.NodeType
                  case "way"      => ProcessOSM.WayType
                  case "relation" => ProcessOSM.RelationType
                }

                val minorVersion = if (prev.data.version == curr.data.version) 1 else 0

                // generate Rows directly for more control over DataFrame schema; toDF will infer these, but let's be
                // explicit
                new GenericRowWithSchema(
                  Array(
                    curr.data.sequence.orNull,
                    _type,
                    prev.data.id,
                    prev.geom.toWKB(4326),
                    curr.geom.toWKB(4326),
                    prev.data.tags,
                    curr.data.tags,
                    prev.data.changeset,
                    curr.data.changeset,
                    prev.data.uid,
                    curr.data.uid,
                    prev.data.user,
                    curr.data.user,
                    prev.data.timestamp,
                    curr.data.timestamp,
                    prev.data.visible.getOrElse(true),
                    curr.data.visible.getOrElse(true),
                    prev.data.version,
                    curr.data.version,
                    -1, // previous minor version is unknown
                    minorVersion
                  ),
                  AugmentedDiffSchema
                ): Row
              case (None, curr) =>
                val _type = curr.data.elementType match {
                  case "node"     => ProcessOSM.NodeType
                  case "way"      => ProcessOSM.WayType
                  case "relation" => ProcessOSM.RelationType
                }

                new GenericRowWithSchema(
                  Array(
                    curr.data.sequence.orNull,
                    _type,
                    curr.data.id,
                    null,
                    curr.geom.toWKB(4326),
                    null,
                    curr.data.tags,
                    null,
                    curr.data.changeset,
                    null,
                    curr.data.uid,
                    null,
                    curr.data.user,
                    null,
                    curr.data.timestamp,
                    null,
                    curr.data.visible.getOrElse(true),
                    null,
                    curr.data.version,
                    null,
                    0
                  ),
                  AugmentedDiffSchema
                ): Row
            }

            val changesetOptions = Map("base_uri" -> changesetSource.toString) ++
              startSequence
                .map(s => Map("start_sequence" -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              endSequence
                .map(s => Map("end_sequence" -> s.toString))
                .getOrElse(Map.empty[String, String])

            val changesets =
              ss.readStream
                .format("changesets")
                .options(changesetOptions)
                .load

            val watermarkedChangesets = changesets
            // changesets can remain open for 24 hours; buy some extra time
            // TODO can projecting into the future (created_at + 24 hours) and coalescing closed_at reduce the number
            // of changesets being tracked?
              .withWatermark("created_at", "25 hours")
              .withColumn("hashtag", explode(hashtags('tags)))
              .select('id as 'changeset, 'hashtag)

            // TODO these don't need to be proper geoms from aug diffs
            val watermarkedGeoms = geoms
              .withColumn("timestamp", to_timestamp('sequence * 60 + 1347432900))
              // geoms are standalone; no need to wait for anything
              .withWatermark("timestamp", "0 seconds")
              .where('_type === ProcessOSM.NodeType)
              .select('timestamp, 'changeset, 'user, 'geom)

            val BASE_ZOOM = 15
            val Cols = 256
            val Rows = 256

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

            val baseQuery = watermarkedGeoms
              .join(watermarkedChangesets, Seq("changeset"))

            val tiledHashtags = baseQuery
              .withColumnRenamed("hashtag", "key")
              .flatMap {
                row =>
                  val key = row.getAs[String]("key")
                  val geom = row.getAs[Array[Byte]]("geom").readWKB
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
                    val filename = s"$key/${path(zoom, sk)}"
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
                            println(s"writing new layer to $uri")
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

                            write(uri, byteStream.toByteArray)
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

                        write(uri, byteStream.toByteArray)
                    }

                    (key, zoom, sk.col, sk.row, feats.size)
                }

                modifiedTiles.iterator
            }

            val query = tiledHashtags
              .withColumnRenamed("_1", "key")
              .withColumnRenamed("_2", "zoom")
              .withColumnRenamed("_3", "x")
              .withColumnRenamed("_4", "y")
              .withColumnRenamed("_5", "featureCount")
              .writeStream
              .queryName("tiled hashtags")
              .format("console")
              .start

            query.awaitTermination()

            ss.stop()
        }
      }
    )
