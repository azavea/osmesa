package osmesa.common.sources

import java.net.URI
import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types._
import osmesa.common.model.AugmentedDiff

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

case class AugmentedDiffStreamBatchTask(baseURI: URI, sequences: Seq[Int])
    extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] =
    AugmentedDiffStreamBatchReader(baseURI, sequences)
}

case class AugmentedDiffStreamBatchReader(baseURI: URI, sequences: Seq[Int])
    extends ReplicationStreamBatchReader[AugmentedDiff](baseURI, sequences) {

  override def schema: StructType = AugmentedDiff.Schema

  override def getSequence(baseURI: URI, sequence: Int): Seq[AugmentedDiff] =
    AugmentedDiffSource.getSequence(baseURI, sequence)
}

case class AugmentedDiffMicroBatchReader(options: DataSourceOptions, checkpointLocation: String)
    extends ReplicationStreamMicroBatchReader(options, checkpointLocation) {

  override def getCurrentSequence: Option[Int] =
    AugmentedDiffSource.getCurrentSequence(baseURI)

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] =
    sequenceRange
      .map(seq =>
        AugmentedDiffStreamBatchTask(baseURI, Seq(seq)).asInstanceOf[DataReaderFactory[Row]])
      .asJava

  private def baseURI: URI =
    options
      .get(Source.BaseURI)
      .asScala
      .map(new URI(_))
      .getOrElse(
        throw new RuntimeException(
          s"${Source.BaseURI} is a required option for ${Source.AugmentedDiffs}"
        )
      )

  override def readSchema(): StructType = AugmentedDiff.Schema
}
