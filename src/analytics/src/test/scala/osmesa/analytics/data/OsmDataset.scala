package osmesa.analytics.data

import osmesa.analytics._
import osmesa.analytics.stats._

import org.apache.spark.sql._

case class ExpectedCounts(added: Int, modified: Int)
case class ExpectedLengths(added: Double, modified: Double)

case class ExpectedUserStats(
  user: TestData.User,
  buildings: ExpectedCounts = ExpectedCounts(0, 0),
  pois: ExpectedCounts = ExpectedCounts(0, 0),
  roads: ExpectedCounts = ExpectedCounts(0, 0),
  waterways: ExpectedCounts = ExpectedCounts(0, 0),
  roadsKm: ExpectedLengths = ExpectedLengths(0.0, 0.0),
  waterwaysKm: ExpectedLengths = ExpectedLengths(0.0, 0.0),
  countries: Set[String] = Set(), // Just names
  hashtags: Set[TestData.Hashtag] = Set()
)

object ExpectedUserStats {
  def validate(e1: Seq[ExpectedUserStats], e2: Seq[ExpectedUserStats]): Unit = {
    def err(msg: String): Unit =
      sys.error(s"Expected user stats don't match: $msg")
    val m1 = e1.map { u => (u.user, u) }.toMap
    val m2 = e2.map { u => (u.user, u) }.toMap

    if(m1.keys.toSet != m2.keys.toSet) {
      err(s"keys different ${m1.keys} vs ${m2.keys}")
    }

    for(k <- m1.keys) {
      val (s1, s2) = (m1(k), m2(k))
      if(s1 != s2) {
        err(s"User $k doesn't match - $s1 vs $s2")
      }
    }
  }
}

case class ExpectedHashtagStats(
  hashtag: TestData.Hashtag,
  buildings: ExpectedCounts = ExpectedCounts(0, 0),
  pois: ExpectedCounts = ExpectedCounts(0, 0),
  roads: ExpectedCounts = ExpectedCounts(0, 0),
  waterways: ExpectedCounts = ExpectedCounts(0, 0),
  roadsKm: ExpectedLengths = ExpectedLengths(0.0, 0.0),
  waterwaysKm: ExpectedLengths = ExpectedLengths(0.0, 0.0),
  users: List[TestData.User] = List(),
  totalEdits: Int = 0
)

object ExpectedHashatagStats {
  def validate(e1: Seq[ExpectedHashtagStats], e2: Seq[ExpectedHashtagStats]): Unit = {
    def err(msg: String): Unit =
      sys.error(s"Expected hashtag stats don't match: $msg")
    val m1 = e1.map { h => (h.hashtag, h) }.toMap
    val m2 = e2.map { h => (h.hashtag, h) }.toMap

    if(m1.keys.toSet != m2.keys.toSet) {
      err(s"keys different ${m1.keys} vs ${m2.keys}")
    }

    for(k <- m1.keys) {
      val (s1, s2) = (m1(k), m2(k))
      if(s1 != s2) {
        err(s"Hashtag $k doesn't match - $s1 vs $s2")
      }
    }
  }
}

trait OsmDataset {
  def history(implicit ss: SparkSession): DataFrame
  def changesets(implicit ss: SparkSession): DataFrame
  def calculatedStats: (Seq[ExpectedUserStats], Seq[ExpectedHashtagStats])
  def expectedStats: Option[(Seq[ExpectedUserStats], Seq[ExpectedHashtagStats])]
}

object OsmDataset {
  type BuildFunc =
    (TestData.Elements, Changes) => Option[(Seq[ExpectedUserStats], Seq[ExpectedHashtagStats])]

  def build(f: BuildFunc): OsmDataset = {
    val elements = new TestData.Elements
    val changes = new Changes
    val stats =
      f(elements, changes)

    // match {
    //     case Some((expectedUserStats, expectedHashtagStats)) =>
    //       // If the test case passed in the expected stats, verify that
    //       // the Changes computed the same expected stats.
    //       val stats @ (calculatedUserStats, calculatedHashtagStats) = changes.stats

    //       ExpectedUserStats.validate(expectedUserStats, calculatedUserStats)
    //       ExpectedHashatagStats.validate(expectedHashtagStats, calculatedHashtagStats)

    //       stats
    //     case None =>
    //       changes.stats
    //   }

    new OsmDataset {
      def history(implicit ss: SparkSession) = TestData.createHistory(changes.historyRows)
      def changesets(implicit ss: SparkSession) = TestData.createChangesets(changes.changesetRows)
      def calculatedStats = changes.stats
      def expectedStats = stats
    }
  }
}
