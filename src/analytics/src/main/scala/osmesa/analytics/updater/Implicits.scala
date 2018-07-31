package osmesa.analytics.updater

import geotrellis.vectortile.{VInt64, VString, Value}

object Implicits {
  implicit def valueToLong(x: Value): Long = x match {
    case y: VInt64  => y.value
    case y: VString => y.value.toLong
  }

  implicit def valueToString(x: Value): String = x match {
    case y: VInt64  => y.value.toString
    case y: VString => y.value
  }
}
