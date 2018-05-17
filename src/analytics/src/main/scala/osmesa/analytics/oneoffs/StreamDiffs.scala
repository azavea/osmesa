package osmesa.analytics.oneoffs

import com.monovore.decline._
import geotrellis.vector.io._
import geotrellis.vector.io.json.JsonFeatureCollectionMap
import geotrellis.vector.{Feature, Geometry}
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import osmesa.common.functions._
import osmesa.common.functions.osm._
import osmesa.common.{AugmentedDiff, ProcessOSM}

import scala.concurrent.duration._

/*
 * Usage example:
 *
 * sbt "project ingest" assembly
 *
 * spark-submit \
 *   --class osmesa.MakeGeometries \
 *   ingest/target/scala-2.11/osmesa-ingest.jar \
 *   --orc=$HOME/data/osm/isle-of-man.orc \
 *   --out=$HOME/data/osm/isle-of-man-geoms.orc \
 */


object StreamDiffs extends CommandApp(
  name = "osmesa-make-geometries",
  header = "Create geometries from an ORC file",
  main = {
    type AugmentedDiffFeature = Feature[Geometry, AugmentedDiff]
    val FeatureCollectionSchema = StructType(
      StructField("id", StringType) ::
        StructField("type", StringType) ::
        StructField("features", ArrayType(
          StructType(
            StructField("id", StringType) ::
              //            StructField("geometry", StructType(
              //              StructField("type", StringType) ::
              //                // StructField("coordinates", ArrayType(FloatType)) ::
              //                StructField("coordinates", StringType) ::
              //                Nil
              //            )) ::
              StructField("geometry", StringType) ::
              StructField("properties", MapType(StringType, StringType, valueContainsNull = false)) ::
              Nil
          )
        )) ::
        Nil)

    /* CLI option handling */
    val orcO = Opts.option[String]("orc", help = "Location of the ORC file to process")
      .withDefault("/Users/seth/src/azavea/augdiff-pipeline/overpass-diffs/overpass-diff-publisher/artificial/")
    val logger = Logger.getLogger(getClass)

    orcO.map { orc =>
      /* Settings compatible for both local and EMR execution */
      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("make-geometries")
        .set("spark.serializer", classOf[org.apache.spark.serializer.KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)

      implicit val ss: SparkSession = SparkSession.builder
        .config(conf)
        .enableHiveSupport
        .getOrCreate

      import ss.implicits._

      /* Silence the damn INFO logger */
      Logger.getRootLogger.setLevel(Level.WARN)

      // TODO read changeset replication from planet.osm.org
      // probably using a custom receiver: https://spark.apache.org/docs/2.3.0/streaming-custom-receivers.html
      // since data isn't available from a "filesystem"
      // see https://github.com/perlundq/yajsync for an rsync implementation
      // OR guess at file names and periodically check (cf osm-replication-streams)

      // read augmented diffs as text for better geometry support (by reading from GeoJSON w/ GeoTrellis)
      val diffs = ss.readStream.option("maxFilesPerTrigger", 1).textFile(orc)

      implicit val augmentedDiffFeatureEncoder: Encoder[(Option[AugmentedDiffFeature], AugmentedDiffFeature)] =
        Encoders.kryo[(Option[AugmentedDiffFeature], AugmentedDiffFeature)]
      implicit val tagsEncoder: Encoder[(String, Map[String, String])] = Encoders.kryo[(String, Map[String, String])]

      val AugmentedDiffSchema = StructType(
        StructField("sequence", LongType) ::
          StructField("_type", ByteType, nullable = false) ::
          StructField("id", LongType, nullable = false) ::
          StructField("prevGeom", BinaryType, nullable = true) ::
          StructField("geom", BinaryType, nullable = true) ::
          StructField("prevTags", MapType(StringType, StringType, valueContainsNull = false), nullable = true) ::
          StructField("tags", MapType(StringType, StringType, valueContainsNull = false), nullable = false) ::
          StructField("prevChangeset", LongType, nullable = true) ::
          StructField("changeset", LongType, nullable = false) ::
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

      val geoms = diffs map {
          line =>
            val features = line
              // Spark doesn't like RS-delimited JSON; perhaps Spray doesn't either
              .replace("\u001e", "")
              .parseGeoJson[JsonFeatureCollectionMap]
              .getAll[AugmentedDiffFeature]

            (features.get("old"), features("new"))
        } map {
          case (Some(prev), curr) =>
            val _type = curr.data.elementType match {
              case "node" => ProcessOSM.NodeType
              case "way" => ProcessOSM.WayType
              case "relation" => ProcessOSM.RelationType
            }

            val minorVersion = if (prev.data.version == curr.data.version) 1 else 0

            // generate Rows directly for more control over DataFrame schema; toDF will infer these, but let's be
            // explicit
            new GenericRowWithSchema(Array(
              curr.data.sequence.orNull,
              _type,
              prev.data.id,
              prev.geom.toWKB(4326),
              curr.geom.toWKB(4326),
              prev.data.tags,
              curr.data.tags,
              prev.data.changeset,
              curr.data.changeset,
              prev.data.timestamp,
              curr.data.timestamp,
              prev.data.visible.getOrElse(true),
              curr.data.visible.getOrElse(true),
              prev.data.version,
              curr.data.version,
              -1, // previous minor version is unknown
              minorVersion), AugmentedDiffSchema): Row
          case (None, curr) =>
            val _type = curr.data.elementType match {
              case "node" => ProcessOSM.NodeType
              case "way" => ProcessOSM.WayType
              case "relation" => ProcessOSM.RelationType
            }

            new GenericRowWithSchema(Array(
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
              curr.data.timestamp,
              null,
              curr.data.visible.getOrElse(true),
              null,
              curr.data.version,
              null,
              0), AugmentedDiffSchema): Row
        }

      // TODO geocode features

      // aggregations are triggered when an event with a later timestamp ("event time") is received
      // in practice, this means that aggregation doesn't occur until the *next* sequence is received
      // the receiver is potentially responsible for "flushing" (with a textFile source, creating an empty file with a
      // lexicographically lower filename appears to seed the "event time" when processing 1 file per trigger)

      val query = geoms
        .withColumn("timestamp", to_timestamp('sequence * 60 + 1347432900))
        // if sequences are received sequentially (and atomically), 0 seconds should suffice; anything received with an
        // earlier timestamp after that point will be dropped
        .withWatermark("timestamp", "0 seconds")
        .select(
          'timestamp,
          'sequence,
          'changeset,
          when(isRoad('tags) and isNew('version, 'minorVersion), ST_Length('geom))
            .otherwise(lit(0)) as 'road_m_added,
          when(isRoad('tags) and !isNew('version, 'minorVersion), abs(ST_Length('geom) - ST_Length('prevGeom)))
            .otherwise(lit(0)) as 'road_m_modified,
          when(isWaterway('tags) and isNew('version, 'minorVersion), ST_Length('geom))
            .otherwise(lit(0)) as 'waterway_m_added,
          when(isWaterway('tags) and !isNew('version, 'minorVersion), abs(ST_Length('geom) - ST_Length('prevGeom)))
            .otherwise(lit(0)) as 'waterway_m_modified,
          when(isRoad('tags) and isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)) as 'roads_added,
          when(isRoad('tags) and !isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)) as 'roads_modified,
          when(isWaterway('tags) and isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)) as 'waterways_added,
          when(isWaterway('tags) and !isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)) as 'waterways_modified,
          when(isBuilding('tags) and isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)) as 'buildings_added,
          when(isBuilding('tags) and !isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)) as 'buildings_modified,
          when(isPOI('tags) and isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)) as 'pois_added,
          when(isPOI('tags) and !isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)) as 'pois_modified)
        .groupBy('timestamp, 'sequence, 'changeset)
        .agg(
          sum('road_m_added / 1000).as('road_km_added),
          sum('road_m_modified / 1000).as('road_km_modified),
          sum('waterway_m_added / 1000).as('waterway_km_added),
          sum('waterway_m_modified / 1000).as('waterway_km_modified),
          sum('roads_added).as('roads_added),
          sum('roads_modified).as('roads_modified),
          sum('waterways_added).as('waterways_added),
          sum('waterways_modified).as('waterways_modified),
          sum('buildings_added).as('buildings_added),
          sum('buildings_modified).as('buildings_modified),
          sum('pois_added).as('pois_added),
          sum('pois_modified).as('pois_modified))
        .writeStream
        .queryName("aggregate statistics by sequence")
//        .outputMode(OutputMode.Append)
        .format("console")
//        .foreach(new ForeachWriter[Row] {
//          var partitionId: Long = -1
//          var version: Long = -1
//
//          def open(partitionId: Long, version: Long): Boolean = {
//            // Called when starting to process one partition of new data in the executor. The version is for data
//            // deduplication when there are failures. When recovering from a failure, some data may be generated
//            // multiple times but they will always have the same version.
//            //
//            //If this method finds using the partitionId and version that this partition has already been processed,
//            // it can return false to skip the further data processing. However, close still will be called for
//            // cleaning up resources.
//
////            println(s"partition id: ${partitionId}, version: ${version}")
//            this.partitionId = partitionId
//            this.version = version
//
//            true
//          }
//
//          def process(row: Row) = {
//            // write string to connection
//            val sequence = row.getAs[Long]("sequence")
//            val changeset = row.getAs[Long]("changeset")
//            val roadKmAdded = row.getAs[Double]("road_km_added")
//            val roadKmModified = row.getAs[Double]("road_km_modified")
//            val waterwayKmAdded = row.getAs[Double]("waterway_km_added")
//            val waterwayKmModified = row.getAs[Double]("waterway_km_modified")
//            val roadsAdded = row.getAs[Long]("roads_added")
//            val roadsModified = row.getAs[Long]("roads_modified")
//            val waterwaysAdded = row.getAs[Long]("waterways_added")
//            val waterwaysModified = row.getAs[Long]("waterways_modified")
//            val buildingsAdded = row.getAs[Long]("buildings_added")
//            val buildingsModified = row.getAs[Long]("buildings_modified")
//            val poisAdded = row.getAs[Long]("pois_added")
//            val poisModified = row.getAs[Long]("pois_modified")
//
//            println(partitionId, version)
//            println(sequence, changeset, roadKmAdded, roadKmModified, waterwayKmAdded, waterwayKmModified, roadsAdded, roadsModified, waterwaysAdded, waterwaysModified, buildingsAdded, buildingsModified, poisAdded, poisModified)
//          }
//
//          def close(errorOrNull: Throwable): Unit = {
//            // close the connection
//          }
//        })
        .start

//      while (true) {
//        println(query.lastProgress)
//        Thread.sleep(5000)
//      }

      //      val actionCounts = tags.groupBy('value).count
      //      val query = actionCounts.writeStream.outputMode("complete").format("console").start()
      //
      query.awaitTermination()

      ss.stop()

      println("Done.")
    }
  }
)
