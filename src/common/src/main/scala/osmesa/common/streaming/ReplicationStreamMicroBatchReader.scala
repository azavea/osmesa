package osmesa.common.streaming

import java.net.URI
import java.util.Optional

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.sources.v2.reader.streaming.{
  MicroBatchReader,
  Offset
}

import scala.compat.java8.OptionConverters._

abstract class ReplicationStreamBatchReader[T](baseURI: URI,
                                               sequence: Int)
    extends DataReader[Row]
    with Logging {
  protected var index: Int = -1
  protected var items: Vector[T] = _

  override def next(): Boolean = {
    index += 1

    if (Option(items).isEmpty) {
      // initialize items from the starting sequence
      items = getSequence(baseURI, sequence).toVector
    }

    index < items.length
  }

  override def close(): Unit = Unit

  protected def getSequence(baseURI: URI, sequence: Int): Seq[T]
}

abstract class ReplicationStreamMicroBatchReader(options: DataSourceOptions,
                                                 checkpointLocation: String)
    extends MicroBatchReader
    with Logging {
  val DefaultBatchSize: Int = 100

  protected val batchSize: Int = options
    .get("batch_size")
    .asScala
    .map(s => s.toInt)
    .getOrElse(DefaultBatchSize)

  protected var startSequence: Option[Int] =
    options.get("start_sequence").asScala.map(_.toInt)
  protected var endSequence: Option[Int] =
    options.get("end_sequence").asScala.map(_.toInt)

  // start offsets are exclusive, so start with the one before what's requested (if provided)
  protected var startOffset: Option[SequenceOffset] =
    startSequence.map(s => SequenceOffset(s - 1))
  protected var endOffset: Option[SequenceOffset] = None

  override def setOffsetRange(start: Optional[Offset],
                              end: Optional[Offset]): Unit = {
    val currentSequence = getCurrentSequence

    startOffset = Some(
      start.asScala
        .map(_.asInstanceOf[SequenceOffset])
        .getOrElse {
          startOffset.getOrElse {
            SequenceOffset(currentSequence - 1)
          }
        }
    )

    endOffset = Some(
      end.asScala
        .map(_.asInstanceOf[SequenceOffset])
        .getOrElse {
          val nextBatch = startOffset.get.sequence + batchSize

          // jump straight to the end, batching if necessary
          endSequence.map(s => SequenceOffset(math.min(s, nextBatch))).getOrElse {
            // jump to the current sequence, batching if necessary
            SequenceOffset(math.min(currentSequence, nextBatch))
          }
        }
    )
  }

  override def getStartOffset: Offset =
    startOffset.getOrElse {
      throw new IllegalStateException("start offset not set")
    }

  override def getEndOffset: Offset =
    endOffset.getOrElse {
      throw new IllegalStateException("end offset not set")
    }

  override def commit(end: Offset): Unit =
    logInfo(s"Commit: $end")

  override def deserializeOffset(json: String): Offset =
    SequenceOffset(json.toInt)

  override def stop(): Unit = Unit

  protected def getCurrentSequence: Int

  // return an inclusive range
  protected def sequenceRange: Range =
    startOffset.get.sequence + 1 to endOffset.get.sequence
}
