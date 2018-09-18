package osmesa.common.streaming

import java.net.URI
import java.util.Optional
import java.sql._

import cats.implicits._
import io.circe.parser._
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}

import scala.compat.java8.OptionConverters._

import osmesa.common.util.DBUtils

abstract class ReplicationStreamMicroBatchReader(options: DataSourceOptions,
                                                 checkpointLocation: String)
    extends MicroBatchReader
    with Logging {

  private def recoverSequence(dbUri: URI, procName: String): Option[Int] = {
    var sequence: Option[Int] = None
    // Odersky endorses the following.
    // https://issues.scala-lang.org/browse/SI-4437
    var connection = null.asInstanceOf[Connection]
    try {
      connection = DBUtils.getJdbcConnection(dbUri)
      val preppedStatement = connection.prepareStatement("SELECT sequence FROM checkpoints WHERE proc_name = ?")
      preppedStatement.setString(1, procName)
      val rs = preppedStatement.executeQuery()
      sequence = if (rs.next()) Some(rs.getInt("sequence")) else None
    } finally {
      connection.close()
    }

    sequence match {
      case Some(0) => None
      case elsewise => elsewise
    }
  }

  private def checkpointSequence(dbUri: URI, sequence: Int): Unit = {
    // Odersky endorses the following.
    // https://issues.scala-lang.org/browse/SI-4437
    var connection = null.asInstanceOf[Connection]
    try {
      connection = DBUtils.getJdbcConnection(dbUri)
      val upsertSequence =
        connection.prepareStatement(
          """
            |INSERT INTO checkpoints (proc_name, sequence)
            |VALUES (?, ?)
            |ON CONFLICT (proc_name)
            |DO UPDATE SET sequence = ?
          """.stripMargin
        )
      upsertSequence.setString(1, procName)
      upsertSequence.setInt(2, sequence)
      upsertSequence.setInt(3, sequence)
      upsertSequence.execute()
    } finally {
      connection.close()
    }
  }

  val DefaultBatchSize: Int =
    SparkEnv.get.conf.getInt("spark.sql.shuffle.partitions", 200)

  protected val batchSize: Int =
    options.getInt("batch_size", DefaultBatchSize)

  protected val databaseUri: Option[URI] =
    options.get("db_uri").asScala.map(new URI(_))

  protected val procName: String = options.get("proc_name").asScala
    .getOrElse(throw new IllegalStateException("Process name required to recover sequence"))

  protected var startSequence: Option[Int] = {
    val start = databaseUri.flatMap { uri =>
      recoverSequence(uri, procName) orElse options.get("start_sequence").asScala.map(_.toInt)
    }
    logInfo(s"Starting with sequence: $start")
    start
  }

  protected var endSequence: Option[Int] =
    options.get("end_sequence").asScala.map(_.toInt)

  // start offsets are exclusive, so start with the one before what's requested (if provided)
  protected var startOffset: Option[SequenceOffset] =
    startSequence.map(s => SequenceOffset(s - 1))

  protected var endOffset: Option[SequenceOffset] = None

  override def setOffsetRange(start: Optional[Offset],
                              stop: Optional[Offset]): Unit = {
    getCurrentSequence match {
      case Some(currentSequence) =>
        val begin =
          start.asScala
            .map(_.asInstanceOf[SequenceOffset])
            .map(_.next)
            .getOrElse {
              startOffset.map(_.next).getOrElse {
                SequenceOffset(currentSequence - 1)
              }
            }

        startOffset = Some(begin)

        endOffset = Some(
          stop.asScala
            .map(_.asInstanceOf[SequenceOffset])
            .map(_.next)
            .getOrElse {
              val nextBatch = begin.sequence + batchSize

              // jump straight to the end, batching if necessary
              endSequence
                .map(s => SequenceOffset(math.min(s, nextBatch)))
                .getOrElse {
                  // jump to the current sequence, batching if necessary
                  SequenceOffset(math.min(currentSequence, nextBatch))
                }
            }
        ).map(s => Seq(s, begin).max)
      case _ =>
        // remote state is currently unknown

        // provided or current
        startOffset = start.asScala
          .map(_.asInstanceOf[SequenceOffset])
          .map(_.next)
          .map(Some(_))
          .getOrElse(startOffset.map(_.next))

        // provided or max(current start, current end) -- no batching to avoid over-reading
        endOffset = stop.asScala
          .map(_.asInstanceOf[SequenceOffset])
          .map(_.next)
          .map(Some(_))
          .getOrElse {
            (startOffset, endOffset) match {
              case (Some(begin), Some(end)) =>
                if (begin > end) {
                  Some(begin.next)
                } else {
                  Some(end.next)
                }
              case (Some(begin), None) =>
                Some(begin.next)
              case (None, Some(end)) =>
                Some(end.next)
              case _ =>
                None
            }
          }
    }
  }

  override def getStartOffset: Offset =
    startOffset.getOrElse {
      throw new IllegalStateException("start offset not set")
    }

  override def getEndOffset: Offset =
    endOffset.getOrElse {
      throw new IllegalStateException("end offset not set")
    }

  override def commit(end: Offset): Unit = {
    databaseUri.foreach {
      val offset = end.asInstanceOf[SequenceOffset]
      logInfo(s"Saving ending sequence of: ${offset.sequence}")
      checkpointSequence(_, offset.sequence)
    }
    logInfo(s"Commit: $end")
  }

  override def deserializeOffset(json: String): Offset = {
    val t = parse(json) match {
      case Left(failure) => throw failure
      case Right(list) =>
        list.as[Seq[Int]].toOption.map(a => SequenceOffset(a.head, a.last == 1))
    }

    t.getOrElse(
      throw new RuntimeException(s"Could not parse serialized offset: ${json}")
    )
  }

  override def stop(): Unit = Unit

  protected def getCurrentSequence: Option[Int]

  // return an inclusive range
  protected def sequenceRange: Range =
    startOffset.get.sequence + 1 to endOffset.get.sequence
}
