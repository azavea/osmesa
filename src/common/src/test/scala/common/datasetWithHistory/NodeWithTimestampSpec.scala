package common.datasetWithHistory

import java.sql.Timestamp

import common.TestEnvironment
import org.scalatest.FunSpec
import osmesa.common._

class NodeWithTimestampSpec extends FunSpec with TestEnvironment {
  import ss.implicits._

  describe("Dataset[Node with Timestamp] with History") {
    import implicits._

    val nodes = asHistory(HistoryDF).nodes

    describe("withValidity") {
      val ds = nodes.withValidity

      it("should exclude `type`") {
        assert(!ds.schema.fieldNames.contains("type"))
      }

      it("should exclude `nds`") {
        assert(!ds.schema.fieldNames.contains("nds"))
      }

      it("should exclude `members`") {
        assert(!ds.schema.fieldNames.contains("members"))
      }

      it("should set validUntil") {
        val el = ds.where('id === 123104496 and 'version === 3).first()

        assert(el.validUntil === Some(Timestamp.valueOf("2008-08-25 17:58:32")))
      }
    }
  }
}
