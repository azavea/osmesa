package osmesa.analytics.streaming

import java.net.URI
import java.util
import java.util.Optional

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

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
    while (index >= changesets.length && currentOffset + 1 < end) {
      // fetch next sequence
      currentOffset += 1
      changesets = ChangesetsSource.getSequence(baseURI, currentOffset.sequence).toVector

      index = 0
    }

    currentOffset < end && index < changesets.length
  }

  override def get(): Row = {
    val changeset = changesets(index)

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

  private val baseURI = new URI(
    options
      .get("base_uri")
      .orElse("https://planet.osm.org/replication/changesets/"))

  private var start: Option[SequenceOffset] = options
    .get("start_sequence")
    .asScala
    .map(s => SequenceOffset(s.toInt))

  private var end: Option[SequenceOffset] = options
    .get("end_sequence")
    .asScala
    .map(s => SequenceOffset(s.toInt))

  private var committed: Option[SequenceOffset] = None

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    this.start = Some(
      start.asScala
        .map(_.asInstanceOf[SequenceOffset])
        .getOrElse(
          committed
            .map(_ + 1)
            .getOrElse(this.start.getOrElse {
              ChangesetsSource.createInitialOffset(baseURI)
            })))

    this.end = Some(
      end.asScala
        .map(_.asInstanceOf[SequenceOffset])
        .getOrElse(committed.map(_ + 2).getOrElse(this.end.getOrElse(this.start.get + 1))))

    if (this.start == this.end) {
      this.end = Some(this.end.get + 1)
    }
  }

  override def getStartOffset: Offset = {
    start.getOrElse {
      throw new IllegalStateException("start offset not set")
    }
  }

  override def getEndOffset: Offset = {
    end.getOrElse {
      throw new IllegalStateException("end offset not set")
    }
  }

  override def deserializeOffset(json: String): Offset =
    SequenceOffset(json.toInt)

  override def commit(end: Offset): Unit =
    committed = Some(end.asInstanceOf[SequenceOffset])

  override def stop(): Unit = Unit

  override def readSchema(): StructType = ChangesetSchema

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] =
    List(
      ChangesetsStreamBatchTask(baseURI, start.get, end.get)
        .asInstanceOf[DataReaderFactory[Row]]).asJava
}
