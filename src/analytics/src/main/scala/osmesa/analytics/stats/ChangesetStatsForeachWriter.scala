package osmesa.analytics.stats

import java.net.URI
import java.sql.{Connection, PreparedStatement, Types}

import io.circe.syntax._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{ForeachWriter, Row}
import vectorpipe.util.DBUtils

class ChangesetStatsForeachWriter(databaseUri: URI,
                                  shouldUpdateUsernames: Boolean = false,
                                  batchSize: Int = 1000)
    extends ForeachWriter[Row]
    with Logging {
  val UpdateChangesetsQuery: String =
    """
      |-- pre-shape the data to avoid repetition
      |WITH data AS (
      |  SELECT
      |    ? AS id,
      |    ? AS user_id,
      |    ?::jsonb AS measurements,
      |    ?::jsonb AS counts,
      |    ? AS total_edits,
      |    ? AS augmented_diffs,
      |    current_timestamp AS updated_at
      |)
      |INSERT INTO changesets AS c (
      |  id,
      |  user_id,
      |  measurements,
      |  counts,
      |  total_edits,
      |  augmented_diffs,
      |  updated_at
      |) SELECT * FROM data
      |ON CONFLICT (id) DO UPDATE
      |SET
      |  user_id = coalesce(EXCLUDED.user_id, c.user_id),
      |  measurements = (
      |    SELECT jsonb_object_agg(key, value)
      |    FROM (
      |      SELECT key, sum(value::numeric) AS value
      |      FROM (
      |        SELECT * from jsonb_each(c.measurements)
      |        UNION ALL
      |        SELECT * from jsonb_each(EXCLUDED.measurements)
      |      ) AS _
      |      WHERE key IS NOT NULL
      |      GROUP BY key
      |    ) AS _
      |  ),
      |  counts = (
      |    SELECT jsonb_object_agg(key, value)
      |    FROM (
      |      SELECT key, sum(value::numeric) AS value
      |      FROM (
      |        SELECT * from jsonb_each(c.counts)
      |        UNION ALL
      |        SELECT * from jsonb_each(EXCLUDED.counts)
      |      ) AS _
      |      WHERE key IS NOT NULL
      |      GROUP BY key
      |    ) AS _
      |  ),
      |  total_edits = c.total_edits + EXCLUDED.total_edits,
      |  augmented_diffs = coalesce(c.augmented_diffs, ARRAY[]::integer[]) || EXCLUDED.augmented_diffs,
      |  updated_at = current_timestamp
      |WHERE c.id = EXCLUDED.id
      |  AND NOT coalesce(c.augmented_diffs, ARRAY[]::integer[]) && EXCLUDED.augmented_diffs
    """.stripMargin

  val UpdateUsersQuery: String =
    """
      |-- pre-shape the data to avoid repetition
      |WITH data AS (
      |  SELECT
      |    ? AS id,
      |    ? AS name
      |)
      |INSERT INTO users AS u (
      |  id,
      |  name
      |) SELECT * FROM data
      |ON CONFLICT (id) DO NOTHING
    """.stripMargin

  val UpdateUsernamesQuery: String =
    """
      |-- pre-shape the data to avoid repetition
      |WITH data AS (
      |  SELECT
      |    ? AS id,
      |    ? AS name
      |)
      |UPDATE users u
      |SET
      |  name = data.name
      |FROM data
      |WHERE u.id = data.id
      |  AND u.name != data.name
    """.stripMargin

  val UpdateChangesetCountriesQuery: String =
    """
      |-- pre-shape the data to avoid repetition
      |WITH data AS (
      |  SELECT
      |    ? AS changeset_id,
      |    id AS country_id,
      |    ? AS edit_count,
      |    ? AS augmented_diffs
      |  FROM countries
      |  WHERE code = ?
      |)
      |INSERT INTO changesets_countries AS cc (
      |  changeset_id,
      |  country_id,
      |  edit_count,
      |  augmented_diffs
      |) SELECT * FROM data
      |ON CONFLICT (changeset_id, country_id) DO UPDATE
      |SET
      |  edit_count = cc.edit_count + EXCLUDED.edit_count,
      |  augmented_diffs = coalesce(cc.augmented_diffs, ARRAY[]::integer[]) || EXCLUDED.augmented_diffs
      |WHERE cc.changeset_id = EXCLUDED.changeset_id
      |  AND NOT coalesce(cc.augmented_diffs, ARRAY[]::integer[]) && EXCLUDED.augmented_diffs
    """.stripMargin

  private var partitionId: Long = _
  private var version: Long = _
  private var connection: Connection = _
  private var updateChangesets: PreparedStatement = _
  private var updateUsers: PreparedStatement = _
  private var updateUsernames: PreparedStatement = _
  private var updateChangesetCountries: PreparedStatement = _
  private var recordCount = 0

  def open(partitionId: Long, version: Long): Boolean = {
    // Called when starting to process one partition of new data in the executor. The version is for data
    // deduplication when there are failures. When recovering from a failure, some data may be generated
    // multiple times but they will always have the same version.
    //
    // If this method finds using the partitionId and version that this partition has already been processed,
    // it can return false to skip the further data processing. However, close still will be called for
    // cleaning up resources.

    this.partitionId = partitionId
    this.version = version
    connection = DBUtils.getJdbcConnection(databaseUri)
    updateChangesets = connection.prepareStatement(UpdateChangesetsQuery)
    updateUsers = connection.prepareStatement(UpdateUsersQuery)
    updateUsernames = connection.prepareStatement(UpdateUsernamesQuery)
    updateChangesetCountries = connection.prepareStatement(UpdateChangesetCountriesQuery)

    true
  }

  def process(row: Row): Unit = {
    val sequence = if (row.schema.exists(_.name == "sequence")) {
      Option(row.getAs[Int]("sequence"))
    } else {
      None
    }
    val changeset = row.getAs[Long]("changeset")
    val uid = if (row.schema.exists(_.name == "uid")) {
      Option(row.getAs[Long]("uid"))
    } else {
      None
    }
    val user = if (row.schema.exists(_.name == "user")) {
      Option(row.getAs[String]("user"))
    } else {
      None
    }
    val measurements = Option(row.getAs[Map[String, Double]]("measurements"))
    val counts = Option(row.getAs[Map[String, Int]]("counts"))
    val totalEdits = row.getAs[Integer]("totalEdits")
    val countries = row.getAs[Map[String, Int]]("countries")

    val augmentedDiffs = connection.createArrayOf(
      "integer",
      sequence.map(s => Array(s.underlying())).getOrElse(Array.empty))

    updateChangesets.setLong(1, changeset)

    uid match {
      case Some(u) => updateChangesets.setLong(2, u)
      case None    => updateChangesets.setNull(2, Types.BIGINT)
    }

    updateChangesets.setString(3, measurements.map(_.asJson.noSpaces).orNull)
    updateChangesets.setString(4, counts.map(_.asJson.noSpaces).orNull)
    updateChangesets.setInt(5, totalEdits)
    updateChangesets.setArray(6, augmentedDiffs)

    updateChangesets.addBatch()

    if (uid.isDefined && user.isDefined) {
      updateUsers.setLong(1, uid.get)
      updateUsers.setString(2, user.get)

      updateUsers.addBatch()

      if (shouldUpdateUsernames) {
        updateUsernames.setLong(1, uid.get)
        updateUsernames.setString(2, user.get)

        updateUsernames.addBatch()
      }
    }

    countries foreach {
      case (code, count) =>
        updateChangesetCountries.setLong(1, changeset)
        updateChangesetCountries.setLong(2, count)
        updateChangesetCountries.setArray(3, augmentedDiffs)
        updateChangesetCountries.setString(4, code)

        updateChangesetCountries.addBatch()
    }

    recordCount += 1
    if (recordCount % batchSize == 0) {
      updateChangesets.executeBatch()
      updateUsers.executeBatch()
      updateUsernames.executeBatch()
      updateChangesetCountries.executeBatch()
    }
  }

  def close(errorOrNull: Throwable): Unit = {
    if (Option(errorOrNull).isEmpty) {
      updateChangesets.executeBatch()
      updateUsers.executeBatch()
      updateUsernames.executeBatch()
      updateChangesetCountries.executeBatch()

      updateChangesets.close()
      updateUsers.close()
      updateUsernames.close()
      updateChangesetCountries.close()
    } else {
      logError("Failed writing partition:", errorOrNull)
      errorOrNull.printStackTrace()
    }

    connection.close()
  }
}
