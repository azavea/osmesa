package osmesa.common.sources

import java.net.URI
import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import osmesa.common.model.Change

import scala.collection.JavaConverters._

case class ChangeStreamBatchTask(baseURI: URI, sequences: Seq[Int]) extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] =
    new ChangeStreamBatchReader(baseURI, sequences)
}

class ChangeStreamBatchReader(baseURI: URI, sequences: Seq[Int])
    extends ReplicationStreamBatchReader[Change](baseURI, sequences) {

  override def getSequence(baseURI: URI, sequence: Int): Seq[Change] =
    ChangeSource.getSequence(baseURI, sequence)
}

case class ChangeMicroBatchReader(options: DataSourceOptions, checkpointLocation: String)
    extends ReplicationStreamMicroBatchReader[Change](options, checkpointLocation) {
  private val baseURI = new URI(
    options
      .get(Source.BaseURI)
      .orElse("https://planet.osm.org/replication/minute/")
  )

  override def getCurrentSequence: Option[Int] =
    ChangeSource.getCurrentSequence(baseURI)

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] =
    sequenceRange
      .map(
        seq => ChangeStreamBatchTask(baseURI, Seq(seq)).asInstanceOf[DataReaderFactory[Row]]
      )
      .asJava
}
