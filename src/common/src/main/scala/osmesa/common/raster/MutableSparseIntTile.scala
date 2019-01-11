package osmesa.common.raster

import geotrellis.raster.{
  ArrayTile,
  CellType,
  IntCellType,
  IntCells,
  IntConstantNoDataCellType,
  IntTileVisitor,
  IntUserDefinedNoDataCellType,
  MutableArrayTile,
  NoDataHandling,
  Tile,
  isData
}

import scala.collection.mutable

class MutableSparseIntTile(val cols: Int,
                           val rows: Int,
                           val values: scala.collection.mutable.LongMap[Int],
                           val cellType: IntCells with NoDataHandling)
    extends MutableArrayTile {
  private val noDataValue = cellType match {
    case IntConstantNoDataCellType        => Int.MinValue
    case IntUserDefinedNoDataCellType(nd) => nd
    case IntCellType                      => 0
  }

  override def updateDouble(i: Int, z: Double): Unit = update(i, z.toInt)

  override def update(i: Int, z: Int): Unit = {
    if (isData(z)) {
      values(i) = z
    } else {
      values.remove(i)
    }
  }

  def interpretAs(newCellType: CellType): Tile = {
    newCellType match {
      case dt: IntCells with NoDataHandling =>
        MutableSparseIntTile(cols, rows, values, dt)
      case _ =>
        withNoData(None).convert(newCellType)
    }
  }

  def withNoData(noDataValue: Option[Double]): Tile =
    MutableSparseIntTile(cols, rows, values, cellType.withNoData(noDataValue))

  override def applyDouble(i: Int): Double = apply(i).toDouble

  override def apply(i: Int): Int = values.getOrElse(i, noDataValue)

  override def copy: ArrayTile = MutableSparseIntTile(cols, rows, values.clone(), cellType)

  // unimplemented because it doesn't make sense in this context (and MutableSparseIntTile can't be instantiated from
  // Array[Byte])
  override def toBytes(): Array[Byte] = ???

  def toMap: Map[Long, Int] = values.toMap

  override def foreachIntVisitor(visitor: IntTileVisitor): Unit = {
    values.foreach {
      case (k, v) =>
        val col = k % cols
        val row = k / cols

        visitor(col.toInt, row.toInt, v)
    }
  }
}

object MutableSparseIntTile {
  def apply(cols: Int,
            rows: Int,
            values: mutable.LongMap[Int] = mutable.LongMap.empty[Int],
            cellType: IntCells with NoDataHandling = IntConstantNoDataCellType) =
    new MutableSparseIntTile(cols, rows, values, cellType)
}
