package osmesa.common.streaming

import java.net.URI
import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types._
import osmesa.common.model.Changeset

import scala.collection.JavaConverters._

case class ChangesetsStreamBatchTask(baseURI: URI, start: SequenceOffset, end: SequenceOffset)
    extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] =
    new ChangesetsStreamBatchReader(baseURI, start, end)
}

class ChangesetsStreamBatchReader(baseURI: URI, start: SequenceOffset, end: SequenceOffset)
    extends ReplicationStreamBatchReader[Changeset](baseURI, start, end) {
  override def getSequence(baseURI: URI, sequence: Int): Seq[Changeset] =
    ChangesetsSource.getSequence(baseURI, sequence)

  override def get(): Row = {
    val changeset = items(index)

    Row(
      currentOffset.sequence,
      changeset.id,
      changeset.createdAt,
      changeset.closedAt.orNull,
      changeset.open,
      changeset.numChanges,
      changeset.user,
      changeset.uid,
      changeset.minLat.orNull,
      changeset.maxLat.orNull,
      changeset.minLon.orNull,
      changeset.maxLon.orNull,
      changeset.commentsCount,
      changeset.tags
    )
  }

  override def close(): Unit = Unit
}

class ChangesetsMicroBatchReader(options: DataSourceOptions, checkpointLocation: String)
    extends ReplicationStreamMicroBatchReader(options, checkpointLocation) {
  // TODO extract me
  val ChangesetSchema = StructType(
    StructField("sequence", IntegerType) ::
      StructField("id", LongType) ::
      StructField("created_at", TimestampType, nullable = false) ::
      StructField("closed_at", TimestampType, nullable = true) ::
      StructField("open", BooleanType, nullable = false) ::
      StructField("num_changes", IntegerType, nullable = false) ::
      StructField("user", StringType, nullable = false) ::
      StructField("uid", LongType, nullable = false) ::
      StructField("min_lat", FloatType, nullable = true) ::
      StructField("max_lat", FloatType, nullable = true) ::
      StructField("min_lon", FloatType, nullable = true) ::
      StructField("max_lon", FloatType, nullable = true) ::
      StructField("comments_count", IntegerType, nullable = false) ::
      StructField("tags",
                  MapType(StringType, StringType, valueContainsNull = false),
                  nullable = false) ::
      Nil)

  private val baseURI = new URI(
    options
      .get("base_uri")
      .orElse("https://planet.osm.org/replication/changesets/"))

  override def getCurrentOffset: SequenceOffset =
    ChangesetsSource.createOffsetForCurrentSequence(baseURI)

  override def readSchema(): StructType = ChangesetSchema

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] =
    List(
      ChangesetsStreamBatchTask(baseURI, start.get, end.get)
        .asInstanceOf[DataReaderFactory[Row]]).asJava
}
