package common.dataset

import common.TestEnvironment
import geotrellis.vector.Extent
import org.apache.spark.sql.Dataset
import org.scalatest.FunSpec
import osmesa.common._

class CoordinatesSpec extends FunSpec with TestEnvironment {
  describe("Dataset[Coordinates]") {
    import implicits._

    val nodes
      : Dataset[traits.Node with traits.Timestamp] with traits.History = asHistory(HistoryDF).nodes

    describe("filter(Extent)") {
      val extent = Extent(-117.923083, 33.811298, -117.920122, 33.814035)
      val ds: Dataset[traits.Node with traits.Timestamp] = nodes.filter(extent)

      it("should filter nodes") {
        assert(ds.count === 13701)
      }
    }
  }
}
