package osmesa.common.streaming

import java.net.URI
import java.util.Optional

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}

import scala.compat.java8.OptionConverters._

abstract class ReplicationStreamBatchReader[T](baseURI: URI,
                                               start: SequenceOffset,
                                               end: SequenceOffset)
    extends DataReader[Row]
    with Logging {
  protected var currentOffset: SequenceOffset = start
  protected var index: Int = -1
  protected var items: Vector[T] = _

  protected def getSequence(baseURI: URI, sequence: Int): Seq[T]

  override def next(): Boolean = {
    index += 1

    if (Option(items).isEmpty) {
      // initialize changesets from the starting sequence
      items = getSequence(baseURI, currentOffset.sequence).toVector
    }

    // fetch next batch of changesets if necessary
    // this is a loop in case sequences contain no changesets
    while (index >= items.length && currentOffset + 1 < end) {
      // fetch next sequence
      currentOffset += 1
      items = getSequence(baseURI, currentOffset.sequence).toVector

      index = 0
    }

    currentOffset < end && index < items.length
  }

  override def close(): Unit = Unit
}

abstract class ReplicationStreamMicroBatchReader(options: DataSourceOptions, checkpointLocation: String)
    extends MicroBatchReader
    with Logging {
  val DefaultBatchSize: Int = 100

  protected val batchSize: Int = options
    .get("batch_size")
    .asScala
    .map(s => s.toInt)
    .getOrElse(DefaultBatchSize)

  protected var start: Option[SequenceOffset] = options
    .get("start_sequence")
    .asScala
    .map(s => SequenceOffset(s.toInt))

  protected var end: Option[SequenceOffset] = options
    .get("end_sequence")
    .asScala
    .map(s => SequenceOffset(s.toInt))

  protected def getCurrentOffset: SequenceOffset

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    // TODO memoize this, valid for 30s at a time
    val currentOffset = getCurrentOffset

    this.start = Some(
      start.asScala
        .map(_.asInstanceOf[SequenceOffset])
        .getOrElse {
          this.start.getOrElse {
            currentOffset - 1
          }
        })

    this.end = Some(
      end.asScala
        .map(_.asInstanceOf[SequenceOffset])
        .getOrElse {
          val next = this.end.map(_ + 1).getOrElse {
            this.start.get + 1
          }

          if (currentOffset > next) {
            SequenceOffset(math.min(currentOffset.sequence, next.sequence + batchSize))
          } else {
            next
          }
        })
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

  override def commit(end: Offset): Unit =
    logInfo(s"Commit: $end")

  override def deserializeOffset(json: String): Offset =
    SequenceOffset(json.toInt)

  override def stop(): Unit = Unit
}
