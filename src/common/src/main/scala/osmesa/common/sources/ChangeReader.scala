package osmesa.common.sources

import java.net.URI
import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory
import osmesa.common.model.Change

import scala.collection.JavaConverters._
import scala.util.Random

case class ChangeReader(options: DataSourceOptions) extends ReplicationReader[Change](options) {
  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    // prevent sequential diffs from being assigned to the same task
    val sequences = Random.shuffle((startSequence to endSequence).toList)

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
