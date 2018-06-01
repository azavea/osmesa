package osmesa.common.streaming

import org.apache.spark.sql.sources.v2.reader.streaming.Offset

// TODO should this include the offset within an individual sequence, since MicroBatches are read one-by-one and
// sequences typically correspond to multiple records
case class SequenceOffset(sequence: Int) extends Offset with Ordered[SequenceOffset] {
  override val json: String = sequence.toString

  def +(increment: Int): SequenceOffset = SequenceOffset(sequence + increment)

  override def compare(that: SequenceOffset): Int = this.sequence - that.sequence
}
