package osmesa.common.streaming

import org.apache.spark.sql.sources.v2.reader.streaming.Offset

// TODO see if including a sub-sequence produces empty batches (to allow immediate flushing of watermarked data)
case class SequenceOffset(sequence: Int)
    extends Offset
    with Ordered[SequenceOffset] {
  override val json: String = sequence.toString

  def +(increment: Int): SequenceOffset = SequenceOffset(sequence + increment)
  def -(decrement: Int): SequenceOffset = SequenceOffset(sequence - decrement)

  override def compare(that: SequenceOffset): Int =
    sequence.compare(that.sequence)
}
