package osmesa.common.sources

import java.util.Optional

import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport, ReadSupport}
import org.apache.spark.sql.types.StructType

class ChangeProvider
    extends DataSourceV2
    with ReadSupport
    with MicroBatchReadSupport
    with DataSourceRegister {
  override def createMicroBatchReader(
    schema: Optional[StructType],
    checkpointLocation: String,
    options: DataSourceOptions
  ): MicroBatchReader = {
    if (schema.isPresent) {
      throw new IllegalStateException(
        "The changes source does not support a user-specified schema."
      )
    }

    ChangeMicroBatchReader(options, checkpointLocation)
  }

  override def shortName(): String = Source.Changes
  override def createReader(options: DataSourceOptions): DataSourceReader =
    ChangeReader(options)
}
