package osmesa.common.sources

import java.net.URI
import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory
import org.apache.spark.sql.types.StructType
import osmesa.common.model.Change

import scala.collection.JavaConverters._

case class ChangeReader(options: DataSourceOptions) extends ReplicationReader(options) {
  override def readSchema(): StructType = Change.Schema

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    val sequences = startSequence to endSequence

    sequences
      .grouped(Math.max(1, sequences.length / partitionCount))
      .toList
      .map(
        ChangeStreamBatchTask(baseURI, _)
          .asInstanceOf[DataReaderFactory[Row]]
      )
      .asJava
  }

  private def baseURI =
    new URI(
      options
        .get(Source.BaseURI)
        .orElse("https://planet.osm.org/replication/minute/"))

  override def getCurrentSequence: Option[Int] = ChangeSource.getCurrentSequence(baseURI)
}
