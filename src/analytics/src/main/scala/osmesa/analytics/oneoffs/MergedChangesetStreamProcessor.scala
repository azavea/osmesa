package osmesa.analytics.oneoffs

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import geotrellis.vector.io._
import geotrellis.vector.io.json.JsonFeatureCollectionMap
import geotrellis.vector.{Feature, Geometry}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import osmesa.common.{AugmentedDiff, ProcessOSM}
import osmesa.common.functions.osm._

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.MergedChangesetStreamProcessor \
 *   ingest/target/scala-2.11/osmesa-analytics.jar \
 *   --augmented-diff-source s3://somewhere/diffs/ \
 */
object MergedChangesetStreamProcessor
    extends CommandApp(
      name = "osmesa-merged-changeset-stream-processor",
      header = "Consume augmented diffs + changesets and join them",
      main = {
        type AugmentedDiffFeature = Feature[Geometry, AugmentedDiff]

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

        (augmentedDiffSourceOpt, changesetSourceOpt, startSequenceOpt, endSequenceOpt).mapN {
          (augmentedDiffSource, changesetSource, startSequence, endSequence) =>
            /* Settings compatible for both local and EMR execution */
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
              ss.readStream.option("maxFilesPerTrigger", 1).textFile(augmentedDiffSource.toString)

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

            val changesetsWithWatermark = changesets
            // changesets can remain open for 24 hours; buy some extra time
            // TODO can projecting into the future (created_at + 24 hours) and coalescing closed_at reduce the number
            // of changesets being tracked?
              .withWatermark("created_at", "25 hours")
              .select('id as 'changeset,
                      'tags.getField("created_by") as 'editor,
                      hashtags('tags) as 'hashtags)

            val geomsWithWatermark = geoms
              .withColumn("timestamp", to_timestamp('sequence * 60 + 1347432900))
              // geoms are standalone; no need to wait for anything
              .withWatermark("timestamp", "0 seconds")
              .select('timestamp, 'changeset, '_type, 'id, 'version, 'minorVersion, 'updated)

            val query = geomsWithWatermark
              .join(changesetsWithWatermark, Seq("changeset"))
              .writeStream
              .queryName("merge features w/ changeset metadata")
              .format("console")
              .start

            query.awaitTermination()

            ss.stop()
        }
      }
    )
