package osmesa.analytics.stats

import java.net.URI
import java.sql.{Connection, PreparedStatement, Timestamp}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{ForeachWriter, Row}
import vectorpipe.util.DBUtils

class ChangesetMetadataForeachWriter(databaseUri: URI,
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
      |    ? AS editor,
      |    ? AS user_id,
      |    ?::timestamp with time zone AS created_at,
      |    ?::timestamp with time zone AS closed_at,
      |    current_timestamp AS updated_at
      |)
      |INSERT INTO changesets AS c (
      |  id,
      |  editor,
      |  user_id,
      |  created_at,
      |  closed_at,
      |  updated_at
      |) SELECT * FROM data
      |ON CONFLICT (id) DO UPDATE
      |SET
      |  editor = EXCLUDED.editor,
      |  user_id = EXCLUDED.user_id,
      |  created_at = EXCLUDED.created_at,
      |  closed_at = EXCLUDED.closed_at,
      |  updated_at = current_timestamp
      |WHERE c.id = EXCLUDED.id
    """.stripMargin

  // https://stackoverflow.com/questions/34708509/how-to-use-returning-with-on-conflict-in-postgresql
  val UpdateChangesetsHashtagsQuery: String =
    """
      |WITH hashtag_data AS (
      |  SELECT
      |    ? AS hashtag
      |),
      |ins AS (
      |  INSERT INTO hashtags AS h (
      |    hashtag
      |  ) SELECT * FROM hashtag_data
      |  ON CONFLICT DO NOTHING
      |  RETURNING id
      |),
      |h AS (
      |  SELECT id
      |  FROM ins
      |  UNION ALL
      |  SELECT id
      |  FROM hashtag_data
      |  JOIN hashtags USING(hashtag)
      |),
      |data AS (
      |  SELECT
      |    ? AS changeset_id,
      |    id AS hashtag_id
      |  FROM h
      |)
      |INSERT INTO changesets_hashtags (
      |  changeset_id,
      |  hashtag_id
      |) SELECT * FROM data
      |ON CONFLICT DO NOTHING
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

  private var partitionId: Long = _
  private var version: Long = _
  private var connection: Connection = _
  private var updateChangesets: PreparedStatement = _
  private var updateUsers: PreparedStatement = _
  private var updateUsernames: PreparedStatement = _
  private var updateChangesetsHashtags: PreparedStatement = _
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
    updateChangesetsHashtags = connection.prepareStatement(UpdateChangesetsHashtagsQuery)

    true
  }

  def process(row: Row): Unit = {
    val id = row.getAs[Long]("id")
    val createdAt = row.getAs[Timestamp]("createdAt")
    val closedAt = row.getAs[Timestamp]("closedAt")
    val user = row.getAs[String]("user")
    val uid = row.getAs[Long]("uid")
    val editor = row.getAs[String]("editor")
    val hashtags = row.getAs[Seq[String]]("hashtags")

    updateChangesets.setLong(1, id)
    updateChangesets.setString(2, editor)
    updateChangesets.setLong(3, uid)
    updateChangesets.setTimestamp(4, createdAt)
    updateChangesets.setTimestamp(5, closedAt)

    updateChangesets.addBatch()

    updateUsers.setLong(1, uid)
    updateUsers.setString(2, user)

    updateUsers.addBatch()

    if (shouldUpdateUsernames) {
      updateUsernames.setLong(1, uid)
      updateUsernames.setString(2, user)

      updateUsernames.addBatch()
    }

    hashtags.foreach { hashtag =>
      updateChangesetsHashtags.setString(1, hashtag)
      updateChangesetsHashtags.setLong(2, id)

      updateChangesetsHashtags.addBatch()
    }

    recordCount += 1
    if (recordCount % batchSize == 0) {
      updateChangesets.executeBatch()
      updateChangesetsHashtags.executeBatch()
      updateUsers.executeBatch()
      updateUsernames.executeBatch()
    }
  }

  def close(errorOrNull: Throwable): Unit = {
    if (Option(errorOrNull).isEmpty) {
      updateChangesets.executeBatch()
      updateChangesetsHashtags.executeBatch()
      updateUsers.executeBatch()
      updateUsernames.executeBatch()

      updateChangesets.close()
      updateChangesetsHashtags.close()
      updateUsers.close()
      updateUsernames.close()
    } else {
      logError("Failed writing partition:", errorOrNull)
      errorOrNull.printStackTrace()
    }

    connection.close()
  }
}
