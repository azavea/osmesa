package common.dataset

import com.vividsolutions.jts.geom.Point
import common.TestEnvironment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.scalatest.FunSpec
import osmesa.common.traits._
import osmesa.common.{traits, _}

class NodeWithTimestampSpec extends FunSpec with TestEnvironment {
  import ss.implicits._

  describe("Dataset[Node with Timestamp]") {
    import osmesa.common.implicits._

    val nodes: Dataset[Node with Timestamp] with History = asHistory(HistoryDF).nodes

    describe("asPoints") {
      val geoms: Dataset[OSMFeature[Point] with Metadata with Timestamp with Visibility] with History = nodes.asPoints.cache
      val geom = geoms.first()

      it("should include Geometry") {
        assert(geoms.schema.fieldNames.contains("geom"))

        assert(geom.isInstanceOf[Geometry[Point]])
      }

      it("should include VersionControl") {
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

    describe("withGeometryChanged") {
      val withGeometryChanged: Dataset[Node with Timestamp with GeometryChanged] = nodes.withGeometryChanged
      val node = withGeometryChanged.first()

      it ("should include GeometryChanged") {
        assert(withGeometryChanged.schema.fieldNames.contains("geometryChanged"))

        assert(node.isInstanceOf[GeometryChanged])
      }
    }
  }
}
