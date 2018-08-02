package osmesa.common.streaming

import java.net.URI
import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types._
import osmesa.common.model.{Changeset, ChangesetSchema}

import scala.collection.JavaConverters._

case class ChangesetsStreamBatchTask(baseURI: URI,
                                     start: Int,
                                     end: Int)
    extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] =
    new ChangesetsStreamBatchReader(baseURI, start, end)
}

class ChangesetsStreamBatchReader(baseURI: URI,
                                  start: Int,
                                  end: Int)
    extends ReplicationStreamBatchReader[Changeset](baseURI, start, end) {
  override def getSequence(baseURI: URI, sequence: Int): Seq[Changeset] =
    ChangesetsSource.getSequence(baseURI, sequence)

  override def get(): Row = {
    val changeset = items(index)

    Row(
      currentSequence,
      changeset.id,
      changeset.createdAt,
      changeset.closedAt.orNull,
      changeset.open,
      changeset.numChanges,
      changeset.user,
      changeset.uid,
      changeset.minLat.orNull,
      changeset.maxLat.orNull,
      changeset.minLon.orNull,
      changeset.maxLon.orNull,
      changeset.commentsCount,
      changeset.tags
    )
  }
}

class ChangesetsMicroBatchReader(options: DataSourceOptions,
                                 checkpointLocation: String)
    extends ReplicationStreamMicroBatchReader(options, checkpointLocation) {
  private val baseURI = new URI(
    options
      .get("base_uri")
      .orElse("https://planet.osm.org/replication/changesets/")
  )

  override def getCurrentOffset: SequenceOffset =
    ChangesetsSource.createOffsetForCurrentSequence(baseURI)

  override def readSchema(): StructType = ChangesetSchema

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] =
    sequenceRange.map(
      seq =>
        ChangesetsStreamBatchTask(baseURI, seq, seq)
          .asInstanceOf[DataReaderFactory[Row]]
    )
      .asJava
}
