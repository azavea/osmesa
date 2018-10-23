package common.dataset

import common.TestEnvironment
import org.apache.spark.sql.functions._
import org.scalatest.FunSpec
import osmesa.common.traits._
import osmesa.common.{traits, _}

class NodeWithTimestampSpec extends FunSpec with TestEnvironment {
  import ss.implicits._

  describe("Dataset[Node with Timestamp]") {
    import osmesa.common.implicits._

    val nodes = asHistory(HistoryDF).nodes

    describe("asPoints") {
      val geoms = nodes.asPoints.cache
      val geom = geoms.first()

      it("should include Geometry") {
        assert(geoms.schema.fieldNames.contains("geom"))

        assert(geom.isInstanceOf[Geometry])
      }

      it("should include Metadata") {
        assert(geoms.schema.fieldNames.contains("changeset"))
        assert(geoms.schema.fieldNames.contains("uid"))
        assert(geoms.schema.fieldNames.contains("user"))

        assert(geom.isInstanceOf[Metadata])
      }

      it("should include Timestamp") {
        assert(geoms.schema.fieldNames.contains("timestamp"))

        assert(geom.isInstanceOf[traits.Timestamp])
      }

      it("should produce the correct number of results") {
        assert(geoms.count === 30487)
      }

      it("should be distinct by changeset") {
        assert(geoms.count === geoms.groupBy('id, 'changeset).agg(first('id)).count)
      }
    }
  }
}
