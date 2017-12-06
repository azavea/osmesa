package osmesa.analytics.data

import org.scalatest._

trait ExpectedStatsValidators extends Matchers {
  def validate(actual: Seq[ExpectedUserStats], expected: Seq[ExpectedUserStats]): Unit = {
    val mA = actual.map { h => (h.user, h) }.toMap
    val mE = expected.map { h => (h.user, h) }.toMap

    withClue(s"Keys different for user stats:") {
      mA.keys.toSet should be (mE.keys.toSet)
    }

    for(k <- mA.keys) {
      val (s1, s2) = (mA(k), mE(k))
      withClue(s"User stats don't match for $k:") {
        s1 should be (s2)
      }
    }
  }

  def validate(actual: Seq[ExpectedHashtagStats], expected: Seq[ExpectedHashtagStats])(implicit d: DummyImplicit): Unit = {
    val mA = actual.map { h => (h.hashtag, h) }.toMap
    val mE = expected.map { h => (h.hashtag, h) }.toMap

    withClue(s"Keys different for hashtag stats:") {
      mA.keys.toSet should be (mE.keys.toSet)
    }

    for(k <- mA.keys) {
      val (s1, s2) = (mA(k), mE(k))
      withClue(s"Hashtag stats don't match:") {
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
}
