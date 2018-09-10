package osmesa.common.streaming

import java.net.URI
import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types._
import osmesa.common.model.{ChangeSchema, Element}

import scala.collection.JavaConverters._

case class ChangeStreamBatchTask(baseURI: URI, sequence: Int)
    extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] =
    new ChangeStreamBatchReader(baseURI, sequence)
}

class ChangeStreamBatchReader(baseURI: URI, sequence: Int)
    extends ReplicationStreamBatchReader[Element](baseURI, sequence) {

  override def getSequence(baseURI: URI, sequence: Int): Seq[Element] =
    ChangeSource.getSequence(baseURI, sequence, false)

  override def get(): Row = {
    val change = items(index)

    val members = change.members.map(
      members => members.map(m => Row(m._type, m.ref, m.role))
    )

    Row(
      sequence,
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
}

class ChangeMicroBatchReader(options: DataSourceOptions,
                             checkpointLocation: String)
    extends ReplicationStreamMicroBatchReader(options, checkpointLocation) {
  private val baseURI = new URI(
    options
      .get("base_uri")
      .orElse("https://planet.osm.org/replication/minute/")
  )

  override def getCurrentSequence: Option[Int] =
    ChangeSource.getCurrentSequence(baseURI, ignoreHttps)

  override def readSchema(): StructType = ChangeSchema

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] =
    sequenceRange
      .map(
        ChangeStreamBatchTask(baseURI, _)
          .asInstanceOf[DataReaderFactory[Row]]
      )
      .asJava
}
