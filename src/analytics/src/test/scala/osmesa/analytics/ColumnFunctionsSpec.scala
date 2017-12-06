package osmesa.analytics

import geotrellis.spark.testkit.TestEnvironment
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalatest._

class ColumnFunctionsSpec extends FunSpec with Matchers with TestEnvironment {
  val sqlContext = new SQLContext(sc)
  implicit val ss = sqlContext.sparkSession
  import ss.implicits._

  describe("hashtags") {
    it("should parse hashtags correctly") {
      val rows =
        Seq(
          Row(Map("comment" -> "#Mapathon, and #another!"))
        )

      val df =
        ss.createDataFrame(
          ss.sparkContext.parallelize(rows),
          StructType(StructField("tags", MapType(StringType, StringType), false) :: Nil)
        )

      val tags =
        df.select(hashtags('tags).as("hashtags")).
          rdd.
          map(_.getAs[Seq[String]]("hashtags")).
          collect.
          flatten.
          toSet

      tags should be (Set("mapathon","another"))
    }
  }
}
