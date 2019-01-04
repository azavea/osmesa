package osmesa.common.relations.utils
import com.vividsolutions.jts.geom.{Coordinate, CoordinateSequence, Envelope}

class PartialCoordinateSequence(sequence: CoordinateSequence, offset: Int)
    extends CoordinateSequence {
  private lazy val _size: Int = sequence.size() - offset

  private lazy val coordinates: Array[Coordinate] = {
    val coords = new Array[Coordinate](size())

    for (i <- 0 until size) {
      coords(i) = getCoordinate(i)
    }

    coords
  }

  override def getDimension: Int = sequence.getDimension

  override def getCoordinate(i: Int): Coordinate = sequence.getCoordinate(offset + i)

  override def getCoordinateCopy(i: Int): Coordinate = sequence.getCoordinateCopy(offset + i)

  override def getCoordinate(index: Int, coord: Coordinate): Unit =
    sequence.getCoordinate(offset + index, coord)

  override def getOrdinate(index: Int, ordinateIndex: Int): Double =
    sequence.getOrdinate(offset + index, ordinateIndex)

  override def setOrdinate(index: Int, ordinateIndex: Int, value: Double): Unit =
    sequence.setOrdinate(offset + index, ordinateIndex, value)

  override def toCoordinateArray: Array[Coordinate] = coordinates

  override def expandEnvelope(env: Envelope): Envelope = {
    for (i <- 0 until size) {
      env.expandToInclude(getX(i), getY(i))
    }

    env
  }

  override def getX(index: Int): Double = sequence.getX(offset + index)

  override def getY(index: Int): Double = sequence.getY(offset + index)

  override def size(): Int = _size

  override def clone(): AnyRef = new PartialCoordinateSequence(sequence, offset)
}
