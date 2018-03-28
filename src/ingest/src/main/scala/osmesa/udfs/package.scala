package osmesa

import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.vector._
import geotrellis.vector.io._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

package object udfs {
  // useful for debugging
  val isValid: UserDefinedFunction = udf((geom: Array[Byte]) => {
    geom match {
      case null => true
      case _ => geom.readWKB.isValid
    }
  })

  private def _reproject(geom: Array[Byte], targetCRS: CRS = WebMercator) =
    geom match {
      case null => null
      case _ => geom.readWKB.reproject(LatLng, targetCRS).toWKB(targetCRS.epsgCode.get)
    }

  // useful for debugging; some geometries that are valid as 4326 are not as 3857
  val reproject: UserDefinedFunction = udf {
    (geom: Array[Byte], targetCRS: CRS) => _reproject(geom, targetCRS)
  }

  val ST_AsText: UserDefinedFunction = udf((geom: Array[Byte]) => {
    geom match {
      case null => ""
      case _ => geom.readWKB.toWKT
    }
  })

  private val _isMultiPolygon = (tags: Map[String, String]) =>
    tags.contains("type") && Set("boundary", "multipolygon").contains(tags("type").toLowerCase)

  val isMultiPolygon: UserDefinedFunction = udf(_isMultiPolygon)
}
