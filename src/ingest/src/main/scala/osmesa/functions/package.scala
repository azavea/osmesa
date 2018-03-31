package osmesa

import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.vector._
import geotrellis.vector.io._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

package object functions {
  private def _reproject(geom: Array[Byte], targetCRS: CRS = WebMercator) =
    geom match {
      case null => null
      case _ => geom.readWKB.reproject(LatLng, targetCRS).toWKB(targetCRS.epsgCode.get)
    }

  // Convert BigDecimals to double
  // Reduces size taken for representation at the expense of some precision loss.
  val asDouble: UserDefinedFunction = udf((bd: java.math.BigDecimal) => {
    Option(bd).map(_.doubleValue).getOrElse(Double.NaN)
  })

  val ST_AsText: UserDefinedFunction = udf((geom: Array[Byte]) => {
    geom match {
      case null => ""
      case _ => geom.readWKB.toWKT
    }
  })

  val ST_IsEmpty: UserDefinedFunction = udf((geom: Array[Byte]) => {
    geom match {
      case null => null
      case _ => geom.readWKB.isEmpty
    }
  })

  val ST_IsValid: UserDefinedFunction = udf((geom: Array[Byte]) => {
    geom match {
      case null => null
      case _ => geom.readWKB.isValid
    }
  })

  val ST_Point: UserDefinedFunction = udf((x: Double, y: Double) =>
    (x, y) match {
      // drop ways with invalid coordinates
      case (_, _) if x.equals(Double.NaN) || y.equals(Double.NaN) => null
      // drop ways that don't contain valid geometries
      case (_, _) => Point(x, y).toWKB(4326)
    }
  )

  val ST_Transform: UserDefinedFunction = udf {
    (geom: Array[Byte], targetCRS: CRS) => _reproject(geom, targetCRS)
  }
}
