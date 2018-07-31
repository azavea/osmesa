package osmesa.common

import java.math.BigDecimal

import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.util.Haversine
import geotrellis.vector._
import geotrellis.vector.io._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import osmesa.common.util._

package object functions {
  // A brief note about style
  // Spark functions are typically defined using snake_case, therefore so are the UDFs
  // internal helper functions use standard Scala naming conventions
  // spatial functions w/ matching signatures use ST_* for familarity

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

  val count_values: UserDefinedFunction = udf {
    (_: Seq[String]).groupBy(identity).mapValues(_.size)
  }

  val flatten: UserDefinedFunction = udf {
    (_: Seq[Seq[String]]).flatten
  }

  val flatten_set: UserDefinedFunction = udf {
    (_: Seq[Seq[String]]).flatten.distinct
  }

  private val _mergeCounts = (a: Map[String, Int], b: Map[String, Int]) =>
    mergeMaps(Option(a).getOrElse(Map.empty[String, Int]), Option(b).getOrElse(Map.empty[String, Int]))(_ + _)

  val merge_counts: UserDefinedFunction = udf(_mergeCounts)

  val merge_sets: UserDefinedFunction = udf { (a: Iterable[String], b: Iterable[String]) =>
    (Option(a).getOrElse(Set.empty).toSet ++ Option(b).getOrElse(Set.empty).toSet).toArray
  }

  val sum_counts: UserDefinedFunction = udf { (counts: Iterable[Map[String, Int]]) =>
    counts.reduce(_mergeCounts(_, _))
  }

  val without: UserDefinedFunction = udf { (list: Seq[String], without: String) =>
    list.filterNot(x => x == without)
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

  val ST_Length: UserDefinedFunction = udf {
    Option(_: Array[Byte]).map { geom =>
      geom.readWKB match {
        case line: Line =>
          line
            .points
            .zip(line.points.tail)
            .foldLeft(0d) { case (acc, (p, c)) =>
              acc + Haversine(p.x, p.y, c.x, c.y)
            }
        case _ => 0d
      }
    }.getOrElse(0d)
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
