package common.datasetWithHistory

import java.sql.Timestamp

import common.TestEnvironment
import org.apache.spark.sql.Dataset
import org.scalatest.FunSpec
import osmesa.common._
import osmesa.common.traits._

class OSMSpec extends FunSpec with TestEnvironment {
  import ss.implicits._

  describe("Dataset[OSM] with History") {
    import implicits._

    val history: Dataset[OSM] with History = asHistory(HistoryDF)
    val timestamp = Timestamp.valueOf("2012-01-01 00:00:00")
    val nodes = history.nodes
    val ways = history.ways
    val relations = history.relations

    val node = nodes.first()
    val way = ways.first()
    val relation = relations.first()

    describe("nodes") {
      it("should produce only nodes") {
        assert(nodes.count === 290861)
      }

      it("should include Tags") {
        assert(nodes.schema.fieldNames.contains("tags"))

        assert(node.isInstanceOf[Tags])
      }
    }

    describe("ways") {
      it("should produce only ways") {
        assert(ways.count === 75849)
      }

      it("should include Tags") {
        assert(ways.schema.fieldNames.contains("tags"))

        assert(way.isInstanceOf[Tags])
      }
    }

    describe("relations") {
      it("should produce only relations") {
        assert(relations.count === 4825)
      }

      it("should include Tags") {
        assert(relations.schema.fieldNames.contains("tags"))

        assert(relation.isInstanceOf[Tags])
      }
    }

    describe("snapshot") {
      val snapshot = history.snapshot(timestamp)
      val ds = snapshot.dataset
      ds.cache

      it("should store the timestamp") {
        assert(snapshot.timestamp === timestamp)
      }

      it("should contain a snapshot of data") {
        assert(ds.where('type === NodeType).count === 29012)
      }
    }

    describe("withValidity") {
      val ds: Dataset[UniversalElement with Validity] with History = history.withValidity

      it("should set validUntil") {
        val node = ds.where('type === NodeType and 'id === 123104496 and 'version === 3).first()

        assert(node.validUntil === Some(Timestamp.valueOf("2008-08-25 17:58:32")))
      }

      it("should include tags for invisible elements") {
        val v2 = ds.where('type === RelationType and 'id === 3605412 and 'version === 2).first()
        val v3 = ds.where('type === RelationType and 'id === 3605412 and 'version === 3).first()

        assert(!v3.visible)
        assert(v2.tags === v3.tags)
      }
    }
  }
}
