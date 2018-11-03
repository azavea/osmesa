package osmesa.common.impl
import org.apache.spark.sql.Dataset
import osmesa.common.traits

class Snapshot[T <: Dataset[_]](val dataset: T) extends traits.Snapshot[T]

object Snapshot {
  def apply[T <: Dataset[_]](dataset: T) = new Snapshot[T](dataset)
}
