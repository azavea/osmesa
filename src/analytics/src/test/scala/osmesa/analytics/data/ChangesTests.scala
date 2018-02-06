package osmesa.analytics.data

import org.scalatest._

// This file checks our expectations and the Changes calculations
// for computing expected stats.

class ChangesTests extends FunSuite with Matchers with ExpectedStatsValidators {
  val f: String => Boolean = { _ => true }
  // val f: String => Boolean = { n => n == "a way over time" }

  for((name, testCase) <- TestCases() if testCase.expectedStats.isDefined && f(name)) {
    test(name) {
      validate(testCase.calculatedStats, testCase.expectedStats.get)
    }
  }
}
