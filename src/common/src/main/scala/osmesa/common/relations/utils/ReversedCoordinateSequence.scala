package osmesa.common.relations.utils
import com.vividsolutions.jts.geom.{Coordinate, CoordinateSequence, Envelope}

class ReversedCoordinateSequence(sequence: CoordinateSequence) extends CoordinateSequence {
  private lazy val coordinates: Array[Coordinate] = {
    val coords = new Array[Coordinate](size())

    for (i <- size - 1 to 0) {
      coords(i) = getCoordinate(i)
    }

    coords
  }

  override def getDimension: Int = sequence.getDimension

  override def getCoordinate(i: Int): Coordinate = sequence.getCoordinate(getIndex(i))

  override def getCoordinateCopy(i: Int): Coordinate = sequence.getCoordinateCopy(getIndex(i))

  override def getCoordinate(index: Int, coord: Coordinate): Unit =
    sequence.getCoordinate(getIndex(index), coord)

  private def getIndex(i: Int): Int = size - 1 - i

  override def size(): Int = sequence.size

  override def getX(index: Int): Double = sequence.getX(getIndex(index))

  override def getY(index: Int): Double = sequence.getY(getIndex(index))

  override def getOrdinate(index: Int, ordinateIndex: Int): Double =
    sequence.getOrdinate(getIndex(index), ordinateIndex)

  override def setOrdinate(index: Int, ordinateIndex: Int, value: Double): Unit =
    sequence.setOrdinate(getIndex(index), ordinateIndex, value)

  override def toCoordinateArray: Array[Coordinate] = coordinates

  override def expandEnvelope(env: Envelope): Envelope = sequence.expandEnvelope(env)

  override def clone(): AnyRef = new ReversedCoordinateSequence(sequence)
}
