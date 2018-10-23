package common.dataset

import common.TestEnvironment
import org.scalatest.FunSpec
import osmesa.common._

class ChangesetSpec extends FunSpec with TestEnvironment {
  describe("Dataset[Changeset]") {
    import implicits._

    val df = asChangesets(ChangesetsDF)

    describe("extractFor") {
      val history = asHistory(HistoryDF)
      val extract = df.extractFor(history)

      it("should produce a set of changesets referenced by elements") {
        assert(extract.count === 11708)
      }
    }
  }
}
