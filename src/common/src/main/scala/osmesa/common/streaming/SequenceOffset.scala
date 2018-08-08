package osmesa.common.streaming

import org.apache.spark.sql.sources.v2.reader.streaming.Offset

case class SequenceOffset(sequence: Int, subSequence: Long = 0)
    extends Offset
    with Ordered[SequenceOffset] {
  override val json: String = s"[$sequence,$subSequence]"

  def +(increment: Int): SequenceOffset = SequenceOffset(sequence + increment)
  def -(decrement: Int): SequenceOffset = SequenceOffset(sequence - decrement)
  def next: SequenceOffset = SequenceOffset(sequence, subSequence + 1)

  override def compare(that: SequenceOffset): Int =
    sequence.compare(that.sequence) match {
      case 0 => subSequence.compare(that.subSequence)
      case x => x
    }

  override def toString: String = s"<SequenceOffset: $sequence.$subSequence>"
}
