package osmesa.common.sources

import java.net.URI
import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory
import org.apache.spark.sql.types.StructType
import osmesa.common.model.Changeset

import scala.collection.JavaConverters._
import scala.util.Random

case class ChangesetReader(options: DataSourceOptions) extends ReplicationReader(options) {
  override def readSchema(): StructType = Changeset.Schema

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    // prevent sequential diffs from being assigned to the same task
    val sequences = Random.shuffle((startSequence to endSequence).toList)

    sequences
      .grouped(Math.max(1, sequences.length / partitionCount))
      .toList
      .map(
        ChangesetStreamBatchTask(baseURI, _)
          .asInstanceOf[DataReaderFactory[Row]]
      )
      .asJava
  }

  override protected def getCurrentSequence: Option[Int] =
    ChangesetSource.getCurrentSequence(baseURI)

  private def baseURI =
    new URI(
      options
        .get(Source.BaseURI)
        .orElse("https://planet.osm.org/replication/changesets/"))
}
