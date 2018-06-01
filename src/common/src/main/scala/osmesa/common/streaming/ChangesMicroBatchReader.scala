package osmesa.common.streaming

import java.net.URI
import java.util
import java.util.Optional

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types._
import osmesa.common.model.Element

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

case class ChangesStreamBatchTask(baseURI: URI, start: SequenceOffset, end: SequenceOffset)
  extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] =
    new ChangesStreamBatchReader(baseURI, start, end)
}

class ChangesStreamBatchReader(baseURI: URI, start: SequenceOffset, end: SequenceOffset)
  extends DataReader[Row]
    with Logging {
  private var currentOffset = start
  private var index = -1
  private var changes: Vector[Element] = _

  override def next(): Boolean = {
    index += 1

    if (Option(changes).isEmpty) {
      // initialize changesets from the starting sequence
      changes = ChangesSource.getSequence(baseURI, currentOffset.sequence).toVector
    }

    // fetch next batch of changesets if necessary
    // this is a loop in case sequences contain no changesets
    while (index >= changes.length && currentOffset + 1 < end) {
      // fetch next sequence
      currentOffset += 1
      changes = ChangesSource.getSequence(baseURI, currentOffset.sequence).toVector

      index = 0
    }

    currentOffset < end && index < changes.length
  }

  override def get(): Row = {
    val change = changes(index)

    val members = change.members.map(members => members.map(m => Row(m._type, m.ref, m.role)))

    Row(
      currentOffset.sequence,
      change._type,
      change.id,
      change.tags,
      change.lat.orNull,
      change.lon.orNull,
      change.nds.orNull,
      members.orNull,
      change.changeset,
      change.timestamp,
      change.uid,
      change.user,
      change.version,
      change.visible
    )
  }

  override def close(): Unit = Unit
}

class ChangesMicroBatchReader(options: DataSourceOptions, checkpointLocation: String)
  extends MicroBatchReader
    with Logging {

  // TODO extract me
  val ChangeSchema = StructType(
    StructField("sequence", IntegerType) ::
      StructField("_type", ByteType, nullable = false) ::
      StructField("id", LongType, nullable = false) ::
      StructField("tags",
        MapType(StringType, StringType, valueContainsNull = false),
        nullable = false) ::
      StructField("lat", DataTypes.createDecimalType(9, 7), nullable = true) ::
      StructField("lon", DataTypes.createDecimalType(10, 7), nullable = true) ::
      StructField("nds", DataTypes.createArrayType(LongType), nullable = true) ::
      StructField(
        "members",
        DataTypes.createArrayType(
          StructType(
            StructField("_type", ByteType, nullable = false) ::
              StructField("ref", LongType, nullable = false) ::
              StructField("role", StringType, nullable = false) ::
              Nil
          )
        ), nullable = true) ::
      StructField("changeset", LongType, nullable = false) ::
      StructField("timestamp", TimestampType, nullable = false) ::
      StructField("uid", LongType, nullable = false) ::
      StructField("user", StringType, nullable = false) ::
      StructField("version", IntegerType, nullable = false) ::
      StructField("visible", BooleanType, nullable = false) ::
      Nil)

  private val baseURI = new URI(
    options
      .get("base_uri")
      .orElse("https://planet.osm.org/replication/minute/"))

  // TODO make lazy in order to make setOffsetRange more readable?
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
              ChangesSource.createInitialOffset(baseURI)
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

  override def readSchema(): StructType = ChangeSchema

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] =
    List(
      ChangesStreamBatchTask(baseURI, start.get, end.get)
        .asInstanceOf[DataReaderFactory[Row]]).asJava
}
