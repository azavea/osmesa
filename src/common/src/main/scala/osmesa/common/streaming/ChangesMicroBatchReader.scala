package osmesa.common.streaming

import java.net.URI
import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types._
import osmesa.common.model.{ChangeSchema, Element}

import scala.collection.JavaConverters._

case class ChangesStreamBatchTask(baseURI: URI,
                                  start: SequenceOffset,
                                  end: SequenceOffset)
    extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] =
    new ChangesStreamBatchReader(baseURI, start, end)
}

class ChangesStreamBatchReader(baseURI: URI,
                               start: SequenceOffset,
                               end: SequenceOffset)
    extends ReplicationStreamBatchReader[Element](baseURI, start, end) {

  override def getSequence(baseURI: URI, sequence: Int): Seq[Element] =
    ChangesSource.getSequence(baseURI, sequence)

  override def get(): Row = {
    val change = items(index)

    val members = change.members.map(
      members => members.map(m => Row(m._type, m.ref, m.role))
    )

    Row(
      currentOffset.sequence,
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

class ChangesMicroBatchReader(options: DataSourceOptions,
                              checkpointLocation: String)
    extends ReplicationStreamMicroBatchReader(options, checkpointLocation) {
  private val baseURI = new URI(
    options
      .get("base_uri")
      .orElse("https://planet.osm.org/replication/minute/")
  )

  override def getCurrentOffset: SequenceOffset =
    ChangesSource.createOffsetForCurrentSequence(baseURI)

  override def readSchema(): StructType = ChangeSchema

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] =
    List(
      ChangesStreamBatchTask(baseURI, start.get, end.get)
        .asInstanceOf[DataReaderFactory[Row]]
    ).asJava
}
