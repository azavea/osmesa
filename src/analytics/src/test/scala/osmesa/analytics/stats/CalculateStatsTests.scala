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

class CalculateStatsTests extends FunSuite with Matchers with TestEnvironment {
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

  test("Test scorecard stats") {
    val history =
      TestData.createHistory(
        Seq(
          Row(
            1L, "node", Map("building" -> "true"), 1.0, 2.0,
            null, null,
            1L, new Timestamp(System.currentTimeMillis()), 1L, "bob", 1L, true
          ),
          Row(
            2L, "relation", Map("highway" -> "motorway"), 1.0, 2.0,
            null, Array(Row("way", 3L, null)),
            1L, new Timestamp(System.currentTimeMillis()), 1L, "bob", 1L, true
          ),
          Row(
            3L, "way", Map(), 1.0, 2.0,
            Array(Row(1L)), null,
            1L, new Timestamp(System.currentTimeMillis()), 1L, "bob", 1L, true
          )
        )
      )

    history.show

    val changesets =
      TestData.createChangesets(
        Seq(
          Row(
            1L, Map("comments" -> "#test"),
            new Timestamp(System.currentTimeMillis() - 10000L), false, new Timestamp(System.currentTimeMillis()),
            1L, 0.0, 1.0, 0.0, 1.0, 1L,
            1L, "bob"
          )
        )
      )

    changesets.show

    val options = CalculateStats.Options(changesetPartitionCount = 8, wayPartitionCount = 8, nodePartitionCount = 8)
    val changesetStats = CalculateStats.computeChangesetStats(history, changesets, options)
    changesetStats.collect().foreach(println)
  }
}
