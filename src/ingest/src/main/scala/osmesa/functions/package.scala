package osmesa

import java.math.BigDecimal

import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.vector._
import geotrellis.vector.io._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

package object functions {
  private def _reproject(geom: Array[Byte], targetCRS: CRS = WebMercator) =
    Option(geom).map(_.readWKB.reproject(LatLng, targetCRS).toWKB(targetCRS.epsgCode.get)).orNull

  // Convert BigDecimals to doubles
  // Reduces size taken for representation at the expense of some precision loss.
  val asDouble: UserDefinedFunction = udf {
    Option(_: BigDecimal).map(_.doubleValue).getOrElse(Double.NaN)
  }

  // Convert BigDecimals to floats
  // Reduces size taken for representation at the expense of more precision loss.
  val asFloat: UserDefinedFunction = udf {
    Option(_: BigDecimal).map(_.floatValue).getOrElse(Float.NaN)
  }

  val ST_AsText: UserDefinedFunction = udf {
    Option(_: Array[Byte]).map(_.readWKB.toWKT).getOrElse("")
  }

  val ST_IsEmpty: UserDefinedFunction = udf {
    Option(_: Array[Byte]).forall(_.readWKB.isEmpty)
  }

  val ST_IsValid: UserDefinedFunction = udf {
    Option(_: Array[Byte]).exists(_.readWKB.isValid)
  }

  val ST_Point: UserDefinedFunction = udf((x: Double, y: Double) =>
    (x, y) match {
      // drop ways with invalid coordinates
      case (_, _) if x.equals(Double.NaN) || y.equals(Double.NaN) => null
      // drop ways that don't contain valid geometries
      case (_, _) => Point(x, y).toWKB(4326)
    }
  )

  val ST_Transform: UserDefinedFunction = udf {
    _reproject(_: Array[Byte], _: CRS)
  }
}
