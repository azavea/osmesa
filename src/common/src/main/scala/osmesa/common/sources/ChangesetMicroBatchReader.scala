package osmesa.common.sources

import java.net.URI
import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import osmesa.common.model.Changeset

import scala.collection.JavaConverters._

case class ChangesetStreamBatchTask(baseURI: URI, sequences: Seq[Int])
    extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] =
    new ChangesetStreamBatchReader(baseURI, sequences)
}

class ChangesetStreamBatchReader(baseURI: URI, sequences: Seq[Int])
    extends ReplicationStreamBatchReader[Changeset](baseURI, sequences) {

  override def getSequence(baseURI: URI, sequence: Int): Seq[Changeset] =
    ChangesetSource.getSequence(baseURI, sequence)
}

class ChangesetMicroBatchReader(options: DataSourceOptions, checkpointLocation: String)
    extends ReplicationStreamMicroBatchReader[Changeset](options, checkpointLocation) {
  private val baseURI = new URI(
    options
      .get(Source.BaseURI)
      .orElse("https://planet.osm.org/replication/changesets/")
  )

  override def getCurrentSequence: Option[Int] =
    ChangesetSource.getCurrentSequence(baseURI)

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] =
    sequenceRange
      .map(
        seq => ChangesetStreamBatchTask(baseURI, Seq(seq)).asInstanceOf[DataReaderFactory[Row]]
      )
      .asJava
}
