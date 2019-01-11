package osmesa.common.sources

import java.net.URI

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.types.StructType
import osmesa.common.ProcessOSM
import osmesa.common.model.AugmentedDiff

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.reflect.runtime.universe.TypeTag

abstract class ReplicationStreamBatchReader[T <: Product: TypeTag](baseURI: URI,
                                                                   sequences: Seq[Int])
    extends DataReader[Row]
    with Logging {
  org.apache.spark.sql.jts.registerTypes()
  private lazy val rowEncoder = RowEncoder(schema).resolveAndBind()
  protected var index: Int = -1
  protected var items: Vector[T] = _
  val Concurrency: Int = 8
  protected def schema: StructType
  private val encoder = ExpressionEncoder[T]

  override def next(): Boolean = {
    index += 1

    if (Option(items).isEmpty) {
      val parSequences = sequences.par
      val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(Concurrency))
      parSequences.tasksupport = taskSupport

      items = parSequences.flatMap(seq => getSequence(baseURI, seq)).toVector

      taskSupport.environment.shutdown()
    }

    index < items.length
  }

  // as of Spark 2.3.x, T in DataReader[T] must be Row
  // ReplicationStreamBatchReader subclasses are currently wired up to produce Product instances with the belief that
  // this constraint will be dropped/changed in the future (plus, they're easier to create)
  //
  // DataReader changes to InputPartitionReader in Spark 2.4 and T is further constrained to InternalRow (allowing
  // rowEncoder to be removed)
  override def get(): Row = rowEncoder.fromRow(encoder.toRow(items(index)))

  override def close(): Unit = Unit

  protected def getSequence(baseURI: URI, sequence: Int): Seq[T]
}
