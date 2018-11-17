package osmesa.common.sources
import org.apache.spark.sql.sources.v2.reader.streaming.Offset

case class SequenceOffset(sequence: Int, pending: Boolean = false)
    extends Offset
    with Ordered[SequenceOffset] {
  override val json: String = s"[$sequence,${pending.compare(false)}]"

  def +(increment: Int): SequenceOffset = SequenceOffset(sequence + increment)
  def -(decrement: Int): SequenceOffset = SequenceOffset(sequence - decrement)
  def next: SequenceOffset = SequenceOffset(sequence, pending = true)

  override def compare(that: SequenceOffset): Int =
    sequence.compare(that.sequence) match {
      case 0 => pending.compare(that.pending)
      case x => x
    }
}
