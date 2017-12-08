package osmesa.analytics.data

import org.scalatest._

// This file checks our expectations and the Changes calculations
// for computing expected stats.

class ChangesTests extends FunSuite with Matchers with ExpectedStatsValidators {
  for((name, testCase) <- TestCases() if testCase.expectedStats.isDefined) {
    test(name) {
      validate(testCase.calculatedStats, testCase.expectedStats.get)
    }
  }
}
