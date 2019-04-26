package osmesa.common

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, FloatType}
import osmesa.common.util._

package object functions {
  // A brief note about style
  // Spark functions are typically defined using snake_case, therefore so are the UDFs
  // internal helper functions use standard Scala naming conventions

  lazy val merge_counts: UserDefinedFunction = udf(_mergeCounts)

  lazy val sum_counts: UserDefinedFunction = udf { counts: Iterable[Map[String, Int]] =>
    counts.reduce(_mergeCounts(_, _))
  }

  // Convert BigDecimals to doubles
  // Reduces size taken for representation at the expense of some precision loss.
  def asDouble(value: Column): Column =
    when(value.isNotNull, value.cast(DoubleType))
      .otherwise(lit(Double.NaN)) as s"asDouble($value)"

  // Convert BigDecimals to floats
  // Reduces size taken for representation at the expense of more precision loss.
  def asFloat(value: Column): Column =
    when(value.isNotNull, value.cast(FloatType))
      .otherwise(lit(Float.NaN)) as s"asFloat($value)"

  val count_values: UserDefinedFunction = udf {
    (_: Seq[String]).groupBy(identity).mapValues(_.size)
  }

  val flatten: UserDefinedFunction = udf {
    (_: Seq[Seq[String]]).flatten
  }

  val flatten_set: UserDefinedFunction = udf {
    (_: Seq[Seq[String]]).flatten.distinct
  }

  val merge_sets: UserDefinedFunction = udf { (a: Iterable[String], b: Iterable[String]) =>
    (Option(a).getOrElse(Set.empty).toSet ++ Option(b).getOrElse(Set.empty).toSet).toArray
  }

  val without: UserDefinedFunction = udf { (list: Seq[String], without: String) =>
    list.filterNot(x => x == without)
  }

  private val _mergeCounts = (a: Map[String, Int], b: Map[String, Int]) =>
    mergeMaps(Option(a).getOrElse(Map.empty[String, Int]),
              Option(b).getOrElse(Map.empty[String, Int]))(_ + _)

  val array_intersects: UserDefinedFunction = udf { (a: Seq[_], b: Seq[_]) => a.intersect(b).nonEmpty}
}
