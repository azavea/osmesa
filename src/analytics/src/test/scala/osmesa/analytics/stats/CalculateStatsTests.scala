package osmesa.analytics.stats

import osmesa.analytics._
import osmesa.analytics.data._

import com.vividsolutions.jts.geom.Coordinate
import geotrellis.spark.testkit.TestEnvironment
import geotrellis.spark.util._
import geotrellis.vector._
import geotrellis.vector.io._
import org.scalatest._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import spray.json._

import java.sql.Timestamp

case class NodeRef(ref: Long)
case class MemberRef(t: String, ref: Long, role: String)

class CalculateStatsTests extends FunSpec with Matchers with TestEnvironment with ExpectedStatsValidators {
  def time[T](msg: String)(f: => T) = {
    val start = System.currentTimeMillis
    val v = f
    val end = System.currentTimeMillis
    println(s"[TIMING] ${msg}: ${java.text.NumberFormat.getIntegerInstance.format(end - start)} ms")
    v
  }

  def write(path: String, txt: String): Unit = {
    import java.nio.file.{Paths, Files}
    import java.nio.charset.StandardCharsets

    Files.write(Paths.get(path), txt.getBytes(StandardCharsets.UTF_8))
  }

  val sqlContext = new SQLContext(sc)
  implicit val ss = sqlContext.sparkSession
  import ss.implicits._

  val options =
    CalculateStats.Options(changesetPartitionCount = 8, wayPartitionCount = 8, nodePartitionCount = 8)

  def runTestCase(ds: OsmDataset): Unit = {
    val (history, changesets) =
      (ds.history, ds.changesets)

    // history.show()

    val (actualUserStats, actualHashtagStats) = CalculateStats.compute(history, changesets, options)
    val (expectedUserStats, expectedHashtagStats) =
      ds.expectedStats match {
        case Some(stats) => stats
        case None => ds.calculatedStats
      }

    validate(
      actualUserStats.collect.map(ExpectedUserStats(_)),
      expectedUserStats
    )

    validate(
      actualHashtagStats.collect.map(ExpectedHashtagStats(_)),
      expectedHashtagStats
    )
  }

  describe("CalcluateStats") {
    // val f: String => Boolean = { _ => true }
    val f: String => Boolean = { s => s == "adding a way of previous nodes" }

    for((name, testCase) <- TestCases() if f(name)) {
      it(s"should handle test case: $name") {
        runTestCase(testCase)
      }
    }
  }
}
