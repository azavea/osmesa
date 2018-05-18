package osmesa.analytics.oneoffs

import java.net.URI
import java.sql.{Connection, DriverManager}

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
import osmesa.common.functions._
import osmesa.common.functions.osm._
import osmesa.common.{AugmentedDiff, ProcessOSM}

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.AugmentedDiffStreamProcessor \
 *   ingest/target/scala-2.11/osmesa-analytics.jar \
 *   --augmented-diff-source s3://somewhere/diffs/ \
 *   --database-url $DATABASE_URL
 */
object AugmentedDiffStreamProcessor extends CommandApp(
  name = "osmesa-augmented-diff-stream-processor",
  header = "Update statistics from streaming augmented diffs",
  main = {
    type AugmentedDiffFeature = Feature[Geometry, AugmentedDiff]

    val augmentedDiffSourceOpt = Opts.option[URI](
      "augmented-diff-source", short = "a", metavar = "uri", help = "Location of augmented diffs to process")
    val databaseUrlOpt = Opts.option[URI](
      "database-url", short = "d", metavar = "database URL", help = "Database URL")

    (augmentedDiffSourceOpt, databaseUrlOpt).mapN { (augmentedDiffSource, databaseUri) =>
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

      // TODO read changeset replication from planet.osm.org
      // probably using a custom receiver: https://spark.apache.org/docs/2.3.0/streaming-custom-receivers.html
      // since data isn't available from a "filesystem"
      // see https://github.com/perlundq/yajsync for an rsync implementation
      // OR guess at file names and periodically check (cf osm-replication-streams)

      // read augmented diffs as text for better geometry support (by reading from GeoJSON w/ GeoTrellis)
      val diffs = ss.readStream.option("maxFilesPerTrigger", 1).textFile(augmentedDiffSource.toString)

      implicit val augmentedDiffFeatureEncoder: Encoder[(Option[AugmentedDiffFeature], AugmentedDiffFeature)] =
        Encoders.kryo[(Option[AugmentedDiffFeature], AugmentedDiffFeature)]

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
            0), AugmentedDiffSchema): Row
      }

      // TODO update footprint MVTs
      // TODO update MVTs (possibly including data from changeset replication)

      // aggregations are triggered when an event with a later timestamp ("event time") is received
      // in practice, this means that aggregation doesn't occur until the *next* sequence is received
      // the receiver is potentially responsible for "flushing" (with a textFile source, creating an empty file with a
      // lexicographically lower filename appears to seed the "event time" when processing 1 file per trigger)

      val query = ProcessOSM.geocode(geoms)
        .withColumn("timestamp", to_timestamp('sequence * 60 + 1347432900))
        // if sequences are received sequentially (and atomically), 0 seconds should suffice; anything received with an
        // earlier timestamp after that point will be dropped
        .withWatermark("timestamp", "0 seconds")
        .select(
          'timestamp,
          'sequence,
          'changeset,
          'uid,
          'user,
          'countries,
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
        .groupBy('timestamp, 'sequence, 'changeset, 'uid, 'user)
        .agg(
          sum('road_m_added / 1000) as 'road_km_added,
          sum('road_m_modified / 1000) as 'road_km_modified,
          sum('waterway_m_added / 1000) as 'waterway_km_added,
          sum('waterway_m_modified / 1000) as 'waterway_km_modified,
          sum('roads_added) as 'roads_added,
          sum('roads_modified) as 'roads_modified,
          sum('waterways_added) as 'waterways_added,
          sum('waterways_modified) as 'waterways_modified,
          sum('buildings_added) as 'buildings_added,
          sum('buildings_modified) as 'buildings_modified,
          sum('pois_added) as 'pois_added,
          sum('pois_modified) as 'pois_modified,
          count_values(flatten(collect_list('countries))) as 'countries)
        .writeStream
        .queryName("aggregate statistics by sequence")
        .foreach(new ForeachWriter[Row] {
          var partitionId: Long = _
          var version: Long = _
          var connection: Connection = _
          val UpdateChangesetsQuery =
            """
              |INSERT INTO changesets AS c (
              |  id,
              |  user_id,
              |  roads_added,
              |  roads_modified,
              |  waterways_added,
              |  waterways_modified,
              |  buildings_added,
              |  buildings_modified,
              |  pois_added,
              |  pois_modified,
              |  road_km_added,
              |  road_km_modified,
              |  waterway_km_added,
              |  waterway_km_modified,
              |  augmented_diffs,
              |  updated_at
              |) VALUES (
              |  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp
              |)
              |ON CONFLICT (id) DO UPDATE
              |SET
              |  roads_added = c.roads_added + ?,
              |  roads_modified = c.roads_modified + ?,
              |  waterways_added = c.waterways_added + ?,
              |  waterways_modified = c.waterways_modified + ?,
              |  buildings_added = c.buildings_added + ?,
              |  buildings_modified = c.buildings_modified + ?,
              |  pois_added = c.pois_added + ?,
              |  pois_modified = c.pois_modified + ?,
              |  road_km_added = c.road_km_added + ?,
              |  road_km_modified = c.road_km_modified + ?,
              |  waterway_km_added = c.waterway_km_added + ?,
              |  waterway_km_modified = c.waterway_km_modified + ?,
              |  augmented_diffs = coalesce(c.augmented_diffs, ARRAY[]::integer[]) || ?,
              |  updated_at = current_timestamp
              |WHERE c.id = ?
              |  AND NOT coalesce(c.augmented_diffs, ARRAY[]::integer[]) && ?
            """.stripMargin

          val UpdateUsersQuery =
            """
              |INSERT INTO users AS u (
              |  id,
              |  name
              |) VALUES (
              |  ?, ?
              |)
              |ON CONFLICT (id) DO UPDATE
              |-- update the user's name if necessary
              |SET
              |  name = ?
              |WHERE u.id = ?
            """.stripMargin

          val UpdateChangesetCountriesQuery =
            """
              |-- pre-shape the data to avoid repetition
              |WITH data AS (
              |  SELECT
              |    ? as changeset_id,
              |    id as country_id,
              |    ? as edit_count
              |  FROM countries
              |  WHERE code = ?
              |)
              |INSERT INTO changesets_countries as cc (
              |  changeset_id,
              |  country_id,
              |  edit_count
              |) SELECT * FROM data
              |ON CONFLICT (changeset_id, country_id) DO UPDATE
              |SET
              |  edit_count = cc.edit_count + EXCLUDED.edit_count
              |WHERE cc.changeset_id = EXCLUDED.changeset_id
            """.stripMargin

          def open(partitionId: Long, version: Long): Boolean = {
            // Called when starting to process one partition of new data in the executor. The version is for data
            // deduplication when there are failures. When recovering from a failure, some data may be generated
            // multiple times but they will always have the same version.
            //
            //If this method finds using the partitionId and version that this partition has already been processed,
            // it can return false to skip the further data processing. However, close still will be called for
            // cleaning up resources.

            this.partitionId = partitionId
            this.version = version

            this.connection = DriverManager.getConnection(s"jdbc:${databaseUri.toString}")

            true
          }

          def process(row: Row) = {
            val sequence = row.getAs[Long]("sequence")
            val changeset = row.getAs[Long]("changeset")
            val uid = row.getAs[Long]("uid")
            val user = row.getAs[String]("user")
            val roadKmAdded = row.getAs[Double]("road_km_added")
            val roadKmModified = row.getAs[Double]("road_km_modified")
            val waterwayKmAdded = row.getAs[Double]("waterway_km_added")
            val waterwayKmModified = row.getAs[Double]("waterway_km_modified")
            val roadsAdded = row.getAs[Long]("roads_added")
            val roadsModified = row.getAs[Long]("roads_modified")
            val waterwaysAdded = row.getAs[Long]("waterways_added")
            val waterwaysModified = row.getAs[Long]("waterways_modified")
            val buildingsAdded = row.getAs[Long]("buildings_added")
            val buildingsModified = row.getAs[Long]("buildings_modified")
            val poisAdded = row.getAs[Long]("pois_added")
            val poisModified = row.getAs[Long]("pois_modified")
            val countries = row.getAs[Map[String, Int]]("countries")

            val updateChangesets = connection.prepareStatement(UpdateChangesetsQuery)

            try {
              updateChangesets.setLong(1, changeset)
              updateChangesets.setLong(2, uid)
              updateChangesets.setLong(3, roadsAdded)
              updateChangesets.setLong(4, roadsModified)
              updateChangesets.setLong(5, waterwaysAdded)
              updateChangesets.setLong(6, waterwaysModified)
              updateChangesets.setLong(7, buildingsAdded)
              updateChangesets.setLong(8, buildingsModified)
              updateChangesets.setLong(9, poisAdded)
              updateChangesets.setLong(10, poisModified)
              updateChangesets.setDouble(11, roadKmAdded)
              updateChangesets.setDouble(12, roadKmModified)
              updateChangesets.setDouble(13, waterwayKmAdded)
              updateChangesets.setDouble(14, waterwayKmModified)
              updateChangesets.setArray(
                15, connection.createArrayOf("integer", Array(sequence.underlying)))
              updateChangesets.setLong(16, roadsAdded)
              updateChangesets.setLong(17, roadsModified)
              updateChangesets.setLong(18, waterwaysAdded)
              updateChangesets.setLong(19, waterwaysModified)
              updateChangesets.setLong(20, buildingsAdded)
              updateChangesets.setLong(21, buildingsModified)
              updateChangesets.setLong(22, poisAdded)
              updateChangesets.setLong(23, poisModified)
              updateChangesets.setDouble(24, roadKmAdded)
              updateChangesets.setDouble(25, roadKmModified)
              updateChangesets.setDouble(26, waterwayKmAdded)
              updateChangesets.setDouble(27, waterwayKmModified)
              updateChangesets.setArray(
                28, connection.createArrayOf("integer", Array(sequence.underlying)))
              updateChangesets.setLong(29, changeset)
              updateChangesets.setArray(
                30, connection.createArrayOf("integer", Array(sequence.underlying)))

              updateChangesets.execute
            } finally {
              updateChangesets.close()
            }

            val updateUsers = connection.prepareStatement(UpdateUsersQuery)

            try {
              updateUsers.setLong(1, uid)
              updateUsers.setString(2, user)
              updateUsers.setString(3, user)
              updateUsers.setLong(4, uid)

              updateUsers.execute
            } finally {
              updateUsers.close()
            }

            countries foreach { case (code, count) =>
              val updateChangesetCountries = connection.prepareStatement(UpdateChangesetCountriesQuery)

              try {
                updateChangesetCountries.setLong(1, changeset)
                updateChangesetCountries.setLong(2, count)
                updateChangesetCountries.setString(3, code)

                updateChangesetCountries.execute
              } finally {
                updateChangesetCountries.close()
              }
            }
          }

          def close(errorOrNull: Throwable): Unit = {
            // close the connection
            this.connection.close()
          }
        })
        .start

      query.awaitTermination()

      ss.stop()
    }
  }
)
