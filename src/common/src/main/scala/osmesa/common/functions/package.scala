package osmesa.common

import java.math.BigDecimal

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import osmesa.common.util._

package object functions {
  // A brief note about style
  // Spark functions are typically defined using snake_case, therefore so are the UDFs
  // internal helper functions use standard Scala naming conventions
  // spatial functions w/ matching signatures use ST_* for familarity

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
}
