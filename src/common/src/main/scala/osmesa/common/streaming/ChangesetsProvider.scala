package osmesa.common.streaming

import java.util.Optional

import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.types.StructType

class ChangesetsProvider extends DataSourceV2 with MicroBatchReadSupport with DataSourceRegister {
  override def createMicroBatchReader(schema: Optional[StructType],
                                      checkpointLocation: String,
                                      options: DataSourceOptions): MicroBatchReader = {
    if (schema.isPresent) {
      throw new IllegalStateException(
        "The changesets source does not support a user-specified schema.")
    }

    new ChangesetsMicroBatchReader(options, checkpointLocation)
  }

  override def shortName(): String = "changesets"
}
