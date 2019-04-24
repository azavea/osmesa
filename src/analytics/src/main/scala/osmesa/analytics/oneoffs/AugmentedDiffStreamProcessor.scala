package osmesa.analytics.oneoffs

import java.net.URI
import java.sql.Connection

import cats.implicits._
import com.monovore.decline._
import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.locationtech.geomesa.spark.jts._
import osmesa.analytics.Analytics
import osmesa.common.ProcessOSM
import osmesa.common.functions._
import osmesa.common.functions.osm._
import osmesa.common.model.ElementWithSequence
import osmesa.common.sources.Source
import osmesa.common.util.DBUtils

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.AugmentedDiffStreamProcessor \
 *   ingest/target/scala-2.11/osmesa-analytics.jar \
 *   --augmented-diff-source s3://somewhere/diffs/ \
 *   --database-uri $DATABASE_URL
 */
object AugmentedDiffStreamProcessor
    extends CommandApp(
      name = "osmesa-augmented-diff-stream-processor",
      header = "Update statistics from streaming augmented diffs",
      main = {
        type AugmentedDiffFeature = Feature[Geometry, ElementWithSequence]

        val augmentedDiffSourceOpt =
          Opts
            .option[URI](
              "augmented-diff-source",
              short = "a",
              metavar = "uri",
              help = "Location of augmented diffs to process"
            )

        val databaseUriOpt =
          Opts
            .option[URI](
              "database-url",
              short = "d",
              metavar = "database URL",
              help = "Database URL (default: $DATABASE_URL environment variable)"
            )
            .orElse(Opts.env[URI]("DATABASE_URL", help = "The URL of the database"))

        val startSequenceOpt =
          Opts
            .option[Int](
              "start-sequence",
              short = "s",
              metavar = "sequence",
              help = "Starting sequence. If absent, the current (remote) sequence will be used."
            )
            .orNone

        val endSequenceOpt =
          Opts
            .option[Int]("end-sequence",
                         short = "e",
                         metavar = "sequence",
                         help = "Ending sequence. If absent, this will be an infinite stream.")
            .orNone

        (augmentedDiffSourceOpt, startSequenceOpt, endSequenceOpt, databaseUriOpt).mapN {
          (augmentedDiffSource, startSequence, endSequence, databaseUri) =>
            implicit val ss: SparkSession = Analytics.sparkSession("AugmentedDiffStreamProcessor")

            import ss.implicits._

            val options = Map(
              Source.BaseURI -> augmentedDiffSource.toString,
              Source.DatabaseURI -> databaseUri.toString,
              Source.ProcessName -> "AugmentedDiffStream"
            ) ++
              startSequence
                .map(s => Map(Source.StartSequence -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              endSequence
                .map(s => Map(Source.EndSequence -> s.toString))
                .getOrElse(Map.empty[String, String])

            val geoms = ss.readStream.format(Source.AugmentedDiffs).options(options).load

            // TODO update footprint MVTs
            // TODO update MVTs (possibly including data from changeset replication)

            // aggregations are triggered when an event with a later timestamp ("event time") is received
            // in practice, this means that aggregation doesn't occur until the *next* sequence is received

            val query = ProcessOSM
              .geocode(geoms)
              .withColumn("timestamp", to_timestamp('sequence * 60 + 1347432900))
              // if sequences are received sequentially (and atomically), 0 seconds should suffice; anything received with an
              // earlier timestamp after that point will be dropped
              .withWatermark("timestamp", "0 seconds")
              .withColumn(
                "delta",
                when(
                  (isRoad('tags) or isWaterway('tags) or isCoastline('tags)),
                  abs(coalesce(
                    when(st_geometryType('geom) === "LineString",
                         st_lengthSphere(st_castToLineString('geom))),
                    lit(0) - coalesce(when(st_geometryType('prevGeom) === "LineString",
                                           st_lengthSphere(st_castToLineString('prevGeom))),
                                      lit(0))
                  ))
                ).otherwise(lit(0))
              )
              .select(
                'timestamp,
                'sequence,
                'changeset,
                'uid,
                'user,
                'countries,
                when(isRoad('tags) and isNew('version, 'minorVersion), 'delta)
                  .otherwise(lit(0)) as 'road_m_added,
                when(isRoad('tags) and !isNew('version, 'minorVersion) and 'visible, 'delta)
                  .otherwise(lit(0)) as 'road_m_modified,
                when(isRoad('tags) and !'visible, 'delta)
                  .otherwise(lit(0)) as 'road_m_deleted,
                when(isWaterway('tags) and isNew('version, 'minorVersion), 'delta)
                  .otherwise(lit(0)) as 'waterway_m_added,
                when(isWaterway('tags) and !isNew('version, 'minorVersion) and 'visible, 'delta)
                  .otherwise(lit(0)) as 'waterway_m_modified,
                when(isWaterway('tags) and !'visible, 'delta)
                  .otherwise(lit(0)) as 'waterway_m_deleted,
                when(isCoastline('tags) and isNew('version, 'minorVersion), 'delta)
                  .otherwise(lit(0)) as 'coastline_m_added,
                when(isCoastline('tags) and !isNew('version, 'minorVersion) and 'visible, 'delta)
                  .otherwise(lit(0)) as 'coastline_m_modified,
                when(isCoastline('tags) and !'visible, 'delta)
                  .otherwise(lit(0)) as 'coastline_m_deleted,
                when(isRoad('tags) and isNew('version, 'minorVersion), lit(1))
                  .otherwise(lit(0)) as 'roads_added,
                when(isRoad('tags) and !isNew('version, 'minorVersion) and 'visible, lit(1))
                  .otherwise(lit(0)) as 'roads_modified,
                when(isRoad('tags) and !'visible, lit(1))
                  .otherwise(lit(0)) as 'roads_deleted,
                when(isWaterway('tags) and isNew('version, 'minorVersion), lit(1))
                  .otherwise(lit(0)) as 'waterways_added,
                when(isWaterway('tags) and !isNew('version, 'minorVersion) and 'visible, lit(1))
                  .otherwise(lit(0)) as 'waterways_modified,
                when(isWaterway('tags) and !'visible, lit(1))
                  .otherwise(lit(0)) as 'waterways_deleted,
                when(isCoastline('tags) and isNew('version, 'minorVersion), lit(1))
                  .otherwise(lit(0)) as 'coastlines_added,
                when(isCoastline('tags) and !isNew('version, 'minorVersion) and 'visible, lit(1))
                  .otherwise(lit(0)) as 'coastlines_modified,
                when(isCoastline('tags) and !'visible, lit(1))
                  .otherwise(lit(0)) as 'coastlines_deleted,
                when(isBuilding('tags) and isNew('version, 'minorVersion), lit(1))
                  .otherwise(lit(0)) as 'buildings_added,
                when(isBuilding('tags) and !isNew('version, 'minorVersion) and 'visible, lit(1))
                  .otherwise(lit(0)) as 'buildings_modified,
                when(isBuilding('tags) and !'visible, lit(1))
                  .otherwise(lit(0)) as 'buildings_deleted,
                when(isPOI('tags) and isNew('version, 'minorVersion), lit(1))
                  .otherwise(lit(0)) as 'pois_added,
                when(isPOI('tags) and !isNew('version, 'minorVersion) and 'visible, lit(1))
                  .otherwise(lit(0)) as 'pois_modified,
                when(isPOI('tags) and !'visible, lit(1))
                  .otherwise(lit(0)) as 'pois_deleted
              )
              .groupBy('timestamp, 'sequence, 'changeset, 'uid, 'user)
              .agg(
                sum('road_m_added / 1000) as 'road_km_added,
                sum('road_m_modified / 1000) as 'road_km_modified,
                sum('road_m_deleted / 1000) as 'road_km_deleted,
                sum('waterway_m_added / 1000) as 'waterway_km_added,
                sum('waterway_m_modified / 1000) as 'waterway_km_modified,
                sum('waterway_m_deleted / 1000) as 'waterway_km_deleted,
                sum('coastline_m_added / 1000) as 'coastline_km_added,
                sum('coastline_m_modified / 1000) as 'coastline_km_modified,
                sum('coastline_m_deleted / 1000) as 'coastline_km_deleted,
                sum('roads_added) as 'roads_added,
                sum('roads_modified) as 'roads_modified,
                sum('roads_deleted) as 'roads_deleted,
                sum('waterways_added) as 'waterways_added,
                sum('waterways_modified) as 'waterways_modified,
                sum('waterways_deleted) as 'waterways_deleted,
                sum('coastlines_added) as 'coastlines_added,
                sum('coastlines_modified) as 'coastlines_modified,
                sum('coastlines_deleted) as 'coastlines_deleted,
                sum('buildings_added) as 'buildings_added,
                sum('buildings_modified) as 'buildings_modified,
                sum('buildings_deleted) as 'buildings_deleted,
                sum('pois_added) as 'pois_added,
                sum('pois_modified) as 'pois_modified,
                sum('pois_deleted) as 'pois_deleted,
                count_values(flatten(collect_list('countries))) as 'countries
              )
              .writeStream
              .queryName("aggregate statistics by sequence")
              .foreach(new ForeachWriter[Row] {
                var partitionId: Long = _
                var version: Long = _
                var connection: Connection = _
                val UpdateChangesetsQuery: String =
                  """
              |-- pre-shape the data to avoid repetition
              |WITH data AS (
              |  SELECT
              |    ? AS id,
              |    ? AS user_id,
              |    ? AS roads_added,
              |    ? AS roads_modified,
              |    ? AS roads_deleted,
              |    ? AS waterways_added,
              |    ? AS waterways_modified,
              |    ? AS waterways_deleted,
              |    ? AS coastlines_added,
              |    ? AS coastlines_modified,
              |    ? AS coastlines_deleted,
              |    ? AS buildings_added,
              |    ? AS buildings_modified,
              |    ? AS buildings_deleted,
              |    ? AS pois_added,
              |    ? AS pois_modified,
              |    ? AS pois_deleted,
              |    ? AS road_km_added,
              |    ? AS road_km_modified,
              |    ? AS road_km_deleted,
              |    ? AS waterway_km_added,
              |    ? AS waterway_km_modified,
              |    ? AS waterway_km_deleted,
              |    ? AS coastline_km_added,
              |    ? AS coastline_km_modified,
              |    ? AS coastline_km_deleted,
              |    ? AS augmented_diffs,
              |    current_timestamp AS updated_at
              |)
              |INSERT INTO changesets AS c (
              |  id,
              |  user_id,
              |  roads_added,
              |  roads_modified,
              |  roads_deleted,
              |  waterways_added,
              |  waterways_modified,
              |  waterways_deleted,
              |  coastlines_added,
              |  coastlines_modified,
              |  coastlines_deleted,
              |  buildings_added,
              |  buildings_modified,
              |  buildings_deleted,
              |  pois_added,
              |  pois_modified,
              |  pois_deleted,
              |  road_km_added,
              |  road_km_modified,
              |  road_km_deleted,
              |  waterway_km_added,
              |  waterway_km_modified,
              |  waterway_km_deleted,
              |  coastline_km_added,
              |  coastline_km_modified,
              |  coastline_km_deleted,
              |  augmented_diffs,
              |  updated_at
              |) SELECT * FROM data
              |ON CONFLICT (id) DO UPDATE
              |SET
              |  roads_added = c.roads_added + coalesce(EXCLUDED.roads_added, 0),
              |  roads_modified = c.roads_modified + coalesce(EXCLUDED.roads_modified, 0),
              |  roads_deleted = c.roads_deleted + coalesce(EXCLUDED.roads_deleted, 0),
              |  waterways_added = c.waterways_added + coalesce(EXCLUDED.waterways_added, 0),
              |  waterways_modified = c.waterways_modified + coalesce(EXCLUDED.waterways_modified, 0),
              |  waterways_deleted = c.waterways_deleted + coalesce(EXCLUDED.waterways_deleted, 0),
              |  coastlines_added = c.coastlines_added + coalesce(EXCLUDED.coastlines_added, 0),
              |  coastlines_modified = c.coastlines_modified + coalesce(EXCLUDED.coastlines_modified, 0),
              |  coastlines_deleted = c.coastlines_deleted + coalesce(EXCLUDED.coastlines_deleted, 0),
              |  buildings_added = c.buildings_added + coalesce(EXCLUDED.buildings_added, 0),
              |  buildings_modified = c.buildings_modified + coalesce(EXCLUDED.buildings_modified, 0),
              |  buildings_deleted = c.buildings_deleted + coalesce(EXCLUDED.buildings_deleted, 0),
              |  pois_added = c.pois_added + coalesce(EXCLUDED.pois_added, 0),
              |  pois_modified = c.pois_modified + coalesce(EXCLUDED.pois_modified, 0),
              |  pois_deleted = c.pois_deleted + coalesce(EXCLUDED.pois_deleted, 0),
              |  road_km_added = c.road_km_added + coalesce(EXCLUDED.road_km_added, 0),
              |  road_km_modified = c.road_km_modified + coalesce(EXCLUDED.road_km_modified, 0),
              |  road_km_deleted = c.road_km_deleted + coalesce(EXCLUDED.road_km_deleted, 0),
              |  waterway_km_added = c.waterway_km_added + coalesce(EXCLUDED.waterway_km_added, 0),
              |  waterway_km_modified = c.waterway_km_modified + coalesce(EXCLUDED.waterway_km_modified, 0),
              |  waterway_km_deleted = c.waterway_km_deleted + coalesce(EXCLUDED.waterway_km_deleted, 0),
              |  coastline_km_added = c.coastline_km_added + coalesce(EXCLUDED.coastline_km_added, 0),
              |  coastline_km_modified = c.coastline_km_modified + coalesce(EXCLUDED.coastline_km_modified, 0),
              |  coastline_km_deleted = c.coastline_km_deleted + coalesce(EXCLUDED.coastline_km_deleted, 0),
              |  augmented_diffs = coalesce(c.augmented_diffs, ARRAY[]::integer[]) || EXCLUDED.augmented_diffs,
              |  updated_at = current_timestamp
              |WHERE c.id = EXCLUDED.id
              |  AND NOT coalesce(c.augmented_diffs, ARRAY[]::integer[]) && EXCLUDED.augmented_diffs
            """.stripMargin

                val UpdateUsersQuery: String =
                  """
              |--pre-shape the data to avoid repetition
              |WITH data AS (
              |  SELECT
              |    ? AS id,
              |    ? AS name
              |)
              |INSERT INTO users AS u (
              |  id,
              |  name
              |) SELECT * FROM data
              |ON CONFLICT (id) DO UPDATE
              |-- update the user's name if necessary
              |SET
              |  name = EXCLUDED.name
              |WHERE u.id = EXCLUDED.id
            """.stripMargin

                val UpdateChangesetCountriesQuery: String =
                  """
              |-- pre-shape the data to avoid repetition
              |WITH data AS (
              |  SELECT
              |    ? AS changeset_id,
              |    id AS country_id,
              |    ? AS edit_count
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
                  connection = DBUtils.getJdbcConnection(databaseUri)
                  true
                }

                def process(row: Row): Unit = {
                  val sequence = row.getAs[Int]("sequence")
                  val changeset = row.getAs[Long]("changeset")
                  val uid = row.getAs[Long]("uid")
                  val user = row.getAs[String]("user")
                  val roadKmAdded = row.getAs[Double]("road_km_added")
                  val roadKmModified = row.getAs[Double]("road_km_modified")
                  val roadKmDeleted = row.getAs[Double]("road_km_deleted")
                  val waterwayKmAdded = row.getAs[Double]("waterway_km_added")
                  val waterwayKmModified = row.getAs[Double]("waterway_km_modified")
                  val waterwayKmDeleted = row.getAs[Double]("waterway_km_deleted")
                  val coastlineKmAdded = row.getAs[Double]("coastline_km_added")
                  val coastlineKmModified = row.getAs[Double]("coastline_km_modified")
                  val coastlineKmDeleted = row.getAs[Double]("coastline_km_deleted")
                  val roadsAdded = row.getAs[Long]("roads_added")
                  val roadsModified = row.getAs[Long]("roads_modified")
                  val roadsDeleted = row.getAs[Long]("roads_deleted")
                  val waterwaysAdded = row.getAs[Long]("waterways_added")
                  val waterwaysModified = row.getAs[Long]("waterways_modified")
                  val waterwaysDeleted = row.getAs[Long]("waterways_deleted")
                  val coastlinesAdded = row.getAs[Long]("coastlines_added")
                  val coastlinesModified = row.getAs[Long]("coastlines_modified")
                  val coastlinesDeleted = row.getAs[Long]("coastlines_deleted")
                  val buildingsAdded = row.getAs[Long]("buildings_added")
                  val buildingsModified = row.getAs[Long]("buildings_modified")
                  val buildingsDeleted = row.getAs[Long]("buildings_deleted")
                  val poisAdded = row.getAs[Long]("pois_added")
                  val poisModified = row.getAs[Long]("pois_modified")
                  val poisDeleted = row.getAs[Long]("pois_deleted")
                  val countries = row.getAs[Map[String, Int]]("countries")

                  val updateChangesets = connection.prepareStatement(UpdateChangesetsQuery)

                  try {
                    updateChangesets.setLong(1, changeset)
                    updateChangesets.setLong(2, uid)
                    updateChangesets.setLong(3, roadsAdded)
                    updateChangesets.setLong(4, roadsModified)
                    updateChangesets.setLong(5, roadsDeleted)
                    updateChangesets.setLong(6, waterwaysAdded)
                    updateChangesets.setLong(7, waterwaysModified)
                    updateChangesets.setLong(8, waterwaysDeleted)
                    updateChangesets.setLong(9, coastlinesAdded)
                    updateChangesets.setLong(10, coastlinesModified)
                    updateChangesets.setLong(11, coastlinesDeleted)
                    updateChangesets.setLong(12, buildingsAdded)
                    updateChangesets.setLong(13, buildingsModified)
                    updateChangesets.setLong(14, buildingsDeleted)
                    updateChangesets.setLong(15, poisAdded)
                    updateChangesets.setLong(16, poisModified)
                    updateChangesets.setLong(17, poisDeleted)
                    updateChangesets.setDouble(18, roadKmAdded)
                    updateChangesets.setDouble(19, roadKmModified)
                    updateChangesets.setDouble(20, roadKmDeleted)
                    updateChangesets.setDouble(21, waterwayKmAdded)
                    updateChangesets.setDouble(22, waterwayKmModified)
                    updateChangesets.setDouble(23, waterwayKmDeleted)
                    updateChangesets.setDouble(24, coastlineKmAdded)
                    updateChangesets.setDouble(25, coastlineKmModified)
                    updateChangesets.setDouble(26, coastlineKmDeleted)
                    updateChangesets
                      .setArray(27, connection.createArrayOf("integer", Array(sequence.underlying)))

                    updateChangesets.execute
                  } finally {
                    updateChangesets.close()
                  }

                  val updateUsers = connection.prepareStatement(UpdateUsersQuery)

                  try {
                    updateUsers.setLong(1, uid)
                    updateUsers.setString(2, user)

                    updateUsers.execute
                  } finally {
                    updateUsers.close()
                  }

                  countries foreach {
                    case (code, count) =>
                      val updateChangesetCountries =
                        connection.prepareStatement(UpdateChangesetCountriesQuery)

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
                  connection.close()
                }
              })
              .start

            query.awaitTermination()

            ss.stop()
        }
      }
    )
