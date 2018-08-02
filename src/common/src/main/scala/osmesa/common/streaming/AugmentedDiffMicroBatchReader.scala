package osmesa.common.streaming

import java.net.URI
import java.util

import geotrellis.vector.io._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Row}
import osmesa.common.ProcessOSM
import osmesa.common.model.{
  AugmentedDiffEncoder,
  AugmentedDiffFeature,
  AugmentedDiffSchema
}

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

case class AugmentedDiffStreamBatchTask(baseURI: URI,
                                        start: Int,
                                        end: Int)
    extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] =
    new AugmentedDiffStreamBatchReader(baseURI, start, end)
}

class AugmentedDiffStreamBatchReader(baseURI: URI,
                                     start: Int,
                                     end: Int)
    extends ReplicationStreamBatchReader[
      (Option[AugmentedDiffFeature], AugmentedDiffFeature)
    ](baseURI, start, end) {
  override def getSequence(
    baseURI: URI,
    sequence: Int
  ): Seq[(Option[AugmentedDiffFeature], AugmentedDiffFeature)] =
    AugmentedDiffSource.getSequence(baseURI, sequence)

  override def get(): Row = {
    implicit val encoder: Encoder[Row] = AugmentedDiffEncoder

    items(index) match {
      case (Some(prev), curr) =>
        val _type = curr.data.elementType match {
          case "node"     => ProcessOSM.NodeType
          case "way"      => ProcessOSM.WayType
          case "relation" => ProcessOSM.RelationType
        }

        val minorVersion = if (prev.data.version == curr.data.version) 1 else 0

        // generate Rows directly for more control over DataFrame schema; toDF will infer these, but let's be
        // explicit
        new GenericRowWithSchema(
          Array(
            curr.data.sequence.orNull,
            _type,
            prev.data.id,
            prev.geom.toWKB(4326),
            curr.geom.toWKB(4326),
            prev.data.tags,
            curr.data.tags,
            prev.data.changeset,
            curr.data.changeset,
            prev.data.uid,
            curr.data.uid,
            prev.data.user,
            curr.data.user,
            prev.data.timestamp,
            curr.data.timestamp,
            prev.data.visible.getOrElse(true),
            curr.data.visible.getOrElse(true),
            prev.data.version,
            curr.data.version,
            -1, // previous minor version is unknown
            minorVersion
          ),
          AugmentedDiffSchema
        ): Row

      case (None, curr) =>
        val _type = curr.data.elementType match {
          case "node"     => ProcessOSM.NodeType
          case "way"      => ProcessOSM.WayType
          case "relation" => ProcessOSM.RelationType
        }

        new GenericRowWithSchema(
          Array(
            curr.data.sequence.orNull,
            _type,
            curr.data.id,
            null,
            curr.geom.toWKB(4326),
            null,
            curr.data.tags,
            null,
            curr.data.changeset,
            null,
            curr.data.uid,
            null,
            curr.data.user,
            null,
            curr.data.timestamp,
            null,
            curr.data.visible.getOrElse(true),
            null,
            curr.data.version,
            null,
            0
          ),
          AugmentedDiffSchema
        ): Row
    }
  }
}

class AugmentedDiffMicroBatchReader(options: DataSourceOptions,
                                    checkpointLocation: String)
    extends ReplicationStreamMicroBatchReader(options, checkpointLocation) {

  private def baseURI: URI =
    options.get("base_uri").asScala.map(new URI(_)).getOrElse(
        throw new RuntimeException(
          "base_uri is a required option for augmented-diffs"))

  override def getCurrentOffset: SequenceOffset =
    AugmentedDiffSource.createOffsetForCurrentSequence(baseURI)

  override def readSchema(): StructType = AugmentedDiffSchema

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] =
    sequenceRange.map(
        seq =>
          AugmentedDiffStreamBatchTask(baseURI, seq, seq)
            .asInstanceOf[DataReaderFactory[Row]]
    )
      .asJava
}
