package osmesa.common.streaming

import java.net.URI
import java.util.Optional

import cats.syntax.either._
import io.circe.parser._
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.sources.v2.reader.streaming.{
  MicroBatchReader,
  Offset
}

import scala.compat.java8.OptionConverters._

abstract class ReplicationStreamBatchReader[T](baseURI: URI, sequence: Int)
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

