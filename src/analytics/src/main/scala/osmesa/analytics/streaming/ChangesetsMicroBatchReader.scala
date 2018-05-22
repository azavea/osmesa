package osmesa.analytics.streaming

import java.net.URI
import java.sql.Timestamp
import java.util
import java.util.Optional

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

case class ChangesetsStreamBatchTask(baseURI: URI, start: SequenceOffset, end: SequenceOffset)
    extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] =
    new ChangesetsStreamBatchReader(baseURI, start, end)
}

class ChangesetsStreamBatchReader(baseURI: URI, start: SequenceOffset, end: SequenceOffset)
    extends DataReader[Row]
    with Logging {
  private var currentOffset = start
  private var index = -1
  private var changesets: Vector[Changeset] = _

  override def next(): Boolean = {
    index += 1

    if (Option(changesets).isEmpty) {
      // initialize changesets from the starting sequence
      changesets = ChangesetsSource.getSequence(baseURI, currentOffset.sequence).toVector
    }

    // fetch next batch of changesets if necessary
    // this is a loop in case sequences contain no changesets
    while (index >= changesets.length && currentOffset < end) {
      // fetch next sequence
      currentOffset += 1
      changesets = ChangesetsSource.getSequence(baseURI, currentOffset.sequence).toVector

      index = 0
    }

    currentOffset < end || (currentOffset == end && index < changesets.length)
  }

  override def get(): Row = {
    val changeset = changesets(index)

    Row(
      currentOffset.sequence,
      changeset.id,
      Timestamp.from(changeset.createdAt.toDate.toInstant),
      changeset.closedAt.map(dt => Timestamp.from(dt.toDate.toInstant)).orNull,
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
    extends MicroBatchReader
    with Logging {

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

  private var start: SequenceOffset = _
  private var end: SequenceOffset = _

  /**
    * Set the desired offset range for reader factories created from this reader. Reader factories
    * will generate only data within (`start`, `end`]; that is, from the first record after `start`
    * to the record with offset `end`.
    *
    * @param start The initial offset to scan from. If not specified, scan from an
    *              implementation-specified start point, such as the earliest available record.
    * @param end   The last offset to include in the scan. If not specified, scan up to an
    *              implementation-defined endpoint, such as the last available offset
    *              or the start offset plus a target batch size.
    */
  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    this.start = start
      .orElse(SequenceOffset(options.getInt("start_sequence", Int.MinValue)))
      .asInstanceOf[SequenceOffset]

    this.end = end
      .orElse(SequenceOffset(options.getInt("end_sequence", Int.MaxValue)))
      .asInstanceOf[SequenceOffset]
  }

  override def getStartOffset: Offset = {
    if (Option(start).isEmpty) {
      throw new IllegalStateException("start offset not set")
    }

    start
  }

  override def getEndOffset: Offset = {
    if (Option(end).isEmpty) {
      throw new IllegalStateException("end offset not set")
    }

    end
  }

  override def deserializeOffset(json: String): Offset =
    SequenceOffset(json.toInt)

  override def commit(end: Offset): Unit = Unit

  override def stop(): Unit = Unit

  override def readSchema(): StructType = ChangesetSchema

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    logDebug("createDataReaderFactories")
    val baseURI = new URI(
      options
        .get("base_uri")
        .orElse("https://planet.osm.org/replication/changesets/"))

    val start = this.start match {
      case SequenceOffset(Int.MinValue) =>
        ChangesetsSource.createInitialOffset(baseURI)
      case s => s
    }

    logInfo(s"createDataReaderFactories, ${start}, ${end}")

    List(ChangesetsStreamBatchTask(baseURI, start, end).asInstanceOf[DataReaderFactory[Row]]).asJava
  }
}
