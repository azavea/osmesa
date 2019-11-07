package osmesa.analytics.stats

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import vectorpipe.util._

package object functions {
  // A brief note about style
  // Spark functions are typically defined using snake_case, therefore so are the UDFs
  // internal helper functions use standard Scala naming conventions

  lazy val merge_measurements: UserDefinedFunction = udf(_mergeDoubleCounts)

  lazy val sum_measurements: UserDefinedFunction = udf { counts: Iterable[Map[String, Double]] =>
    Option(counts.reduce(_mergeDoubleCounts)).filter(_.nonEmpty).orNull
  }

  lazy val sum_count_values: UserDefinedFunction = udf { counts: Map[String, Int] =>
    counts.values.sum
  }

  lazy val simplify_measurements: UserDefinedFunction = udf { counts: Map[String, Double] =>
    counts.filter(_._2 != 0)
  }

  lazy val simplify_counts: UserDefinedFunction = udf { counts: Map[String, Int] =>
    counts.filter(_._2 != 0)
  }

  private val _mergeIntCounts = (a: Map[String, Int], b: Map[String, Int]) =>
    mergeMaps(Option(a).getOrElse(Map.empty),
      Option(b).getOrElse(Map.empty))(_ + _)

  private val _mergeDoubleCounts = (a: Map[String, Double], b: Map[String, Double]) =>
    mergeMaps(Option(a).getOrElse(Map.empty),
      Option(b).getOrElse(Map.empty))(_ + _)
}
