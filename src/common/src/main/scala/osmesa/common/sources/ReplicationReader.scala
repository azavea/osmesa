package osmesa.common.sources

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.types.StructType

import scala.compat.java8.OptionConverters._
import scala.reflect.runtime.universe.TypeTag

abstract class ReplicationReader[T <: Product: TypeTag](options: DataSourceOptions)
    extends DataSourceReader {
  private lazy val schema: StructType = ExpressionEncoder[T].schema

  val DefaultPartitionCount: Int =
    SparkEnv.get.conf
      .getInt(SQLConf.SHUFFLE_PARTITIONS.key, SQLConf.SHUFFLE_PARTITIONS.defaultValue.get)

  protected val partitionCount: Int =
    options.getInt(Source.PartitionCount, DefaultPartitionCount)

  protected var endSequence: Int =
    options
      .get(Source.EndSequence)
      .asScala
      .map(s => s.toInt - 1)
      .getOrElse(getCurrentSequence
        .getOrElse(throw new RuntimeException("Could not determine end sequence.")))

  override def readSchema(): StructType = schema

  protected def startSequence: Int =
    options
      .get(Source.StartSequence)
      .asScala
      .map(s => s.toInt)
      .getOrElse(getCurrentSequence
        .getOrElse(throw new RuntimeException("Could not determine start sequence.")))

  protected def getCurrentSequence: Option[Int]
}
