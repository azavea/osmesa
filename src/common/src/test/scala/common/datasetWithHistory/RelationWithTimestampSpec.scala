package common.datasetWithHistory

import java.sql.Timestamp

import common.TestEnvironment
import org.scalatest.FunSpec
import osmesa.common._

class RelationWithTimestampSpec extends FunSpec with TestEnvironment {
  import ss.implicits._

  describe("Dataset[Relation with Timestamp] with History") {
    import implicits._

    val relations = asHistory(HistoryDF).relations

    describe("withValidity") {
      val ds = relations.withValidity

      it("should exclude `type`") {
        assert(!ds.schema.fieldNames.contains("type"))
      }

      it("should exclude coordinates") {
        assert(!ds.schema.fieldNames.contains("lat"))
        assert(!ds.schema.fieldNames.contains("lon"))
      }

      it("should exclude `nds`") {
        assert(!ds.schema.fieldNames.contains("nds"))
      }

      it("should set validUntil") {
        val el = ds.where('id === 3605412 and 'version === 1).first()

        assert(el.validUntil === Some(Timestamp.valueOf("2015-04-04 09:04:23")))
      }

      it("should set geometryChanged") {
        pending
      }
    }
  }
}
