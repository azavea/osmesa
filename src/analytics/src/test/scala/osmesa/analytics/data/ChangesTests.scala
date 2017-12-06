package osmesa.analytics.data

import org.scalatest._

// This file checks our expectations and the Changes calculations
// for computing expected stats.
// Any test case that explicitly states it's expected stats should be added here.

class ChangesTests extends FunSuite with Matchers with ExpectedStatsValidators {
  def validate(data: OsmDataset): Unit = {
    assume(data.expectedStats.isDefined, "Can only run this against test cases that have expected stats")
    validate(data.calculatedStats, data.expectedStats.get)
  }

  test("testcase") {
    validate(TestCases.createWayThenNodeChange)
  }
}
