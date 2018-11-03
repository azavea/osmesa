package osmesa.common.impl
import java.sql.Timestamp

import org.apache.spark.sql.Dataset
import osmesa.common.traits

final case class SnapshotWithTimestamp[T <: Dataset[_]](
    override val dataset: T,
    timestamp: Timestamp
) extends Snapshot(dataset)
    with traits.Timestamp
