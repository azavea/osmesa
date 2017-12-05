package osmesa.analytics

import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.runtime.universe.TypeTag

/**
 * UDFs for working with OSM data in Spark DataFrames.
 *
 * @author sfitch
 * @since 4/3/17
 */
trait ColumnFunctions {
  private implicit def arrayEnc[T: TypeTag]: Encoder[Array[T]] = ExpressionEncoder()
  private implicit def boolEnc: Encoder[Boolean] = ExpressionEncoder()

  def hashtags(col: Column): TypedColumn[Any, Array[String]] =
    udf[Array[String], Map[String, String]]({ tags =>
      val HashtagSet = """#([^\u2000-\u206F\u2E00-\u2E7F\s\\'!"#$%()*,.\/:;<=>?@\[\]^{|}~]+)""".r
      var hashtags = List[String]()
      tags.get("comment") match {
        case Some(s) =>
          for (m <- HashtagSet.findAllMatchIn(s)) {
            hashtags = hashtags :+ m.group(1).toLowerCase
          }
        case None => // pass
      }

      hashtags.toArray
    }).apply(col).as[Array[String]]

  def tagKeys(col: Column): TypedColumn[Any, Array[String]] =
    udf[Array[String], Map[String, String]]({ tags =>
      tags.keys.toArray
    }).apply(col).as[Array[String]]

  def isRoad(col: Column): TypedColumn[Any, Boolean] =
    udf[Boolean, Map[String, String]]({ tags =>
      tags.get("highway") match {
        case Some(v) => Constants.ROAD_VALUES.contains(v)
        case None => false
      }
    }).apply(col).as[Boolean]

  def isBuilding(col: Column): TypedColumn[Any, Boolean] =
    udf[Boolean, Map[String, String]]({ tags =>
      tags contains "building"
    }).apply(col).as[Boolean]

  def isPOI(col: Column): TypedColumn[Any, Boolean] =
    udf[Boolean, Map[String, String]]({ tags =>
      tags contains "amenity"
    }).apply(col).as[Boolean]

  def isWaterway(col: Column): TypedColumn[Any, Boolean] =
    udf[Boolean, Map[String, String]]({ tags =>
      tags.get("waterway") match {
        case Some(v) => Constants.WATERWAY_VALUES.contains(v)
        case None => false
      }
    }).apply(col).as[Boolean]
}
