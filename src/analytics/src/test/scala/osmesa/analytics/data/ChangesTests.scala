package osmesa.analytics.data

import org.scalatest._

// This file checks our expectations and the Changes calculations
// for computing expected stats.
// Any test case that explicitly states it's expected stats should be added here.

class ChangesTests extends FunSuite with Matchers {
  def validate(actual: Seq[ExpectedUserStats], expected: Seq[ExpectedUserStats]): Unit = {
    val mA = actual.map { h => (h.user, h) }.toMap
    val mE = expected.map { h => (h.user, h) }.toMap

    withClue(s"Keys different for user stats") {
      mA.keys.toSet should be (mE.keys.toSet)
    }

    for(k <- mA.keys) {
      val (s1, s2) = (mA(k), mE(k))
      withClue(s"User stats don't match") {
        s1 should be (s2)
      }
    }
  }

  def validate(actual: Seq[ExpectedHashtagStats], expected: Seq[ExpectedHashtagStats])(implicit d: DummyImplicit): Unit = {
    val mA = actual.map { h => (h.hashtag, h) }.toMap
    val mE = expected.map { h => (h.hashtag, h) }.toMap

    withClue(s"Keys different for hashtag stats") {
      mA.keys.toSet should be (mE.keys.toSet)
    }

    for(k <- mA.keys) {
      val (s1, s2) = (mA(k), mE(k))
      withClue(s"Hashtag stats don't match") {
        s1 should be (s2)
      }
    }
  }

  def validate(
    actual: (Seq[ExpectedUserStats], Seq[ExpectedHashtagStats]),
    expected: (Seq[ExpectedUserStats], Seq[ExpectedHashtagStats])
  ): Unit = {
    validate(actual._1, expected._1)
    validate(actual._2, expected._2)
  }

  def validate(data: OsmDataset): Unit = {
    assume(data.expectedStats.isDefined, "Can only run this against test cases that have expected stats")
    validate(data.calculatedStats, data.expectedStats.get)
  }

  test("testcase") {
    validate(TestCases.createWayThenNodeChange)
  }
}
