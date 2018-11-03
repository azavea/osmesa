package common.datasetWithHistory

import common.TestEnvironment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.first
import org.scalatest.FunSpec
import osmesa.common._
import osmesa.common.traits._

class WayWithTimestampSpec extends FunSpec with TestEnvironment {
  import ss.implicits._

  describe("Dataset[Way with Timestamp] with History") {
    import implicits._

    val history: Dataset[OSM] with History = asHistory(HistoryDF)
    val ways: Dataset[Way with Timestamp] with History = history.ways

    describe("withArea") {
      val withArea: Dataset[Way with Timestamp with Area] = ways.withArea
      val way = withArea.first()

      it("should include Area") {
        assert(withArea.schema.fieldNames.contains("isArea"))

        assert(way.isInstanceOf[Area])
      }

      it("should treat area tags as an area") {
        val area = withArea.where('tags.getField("building") === "yes").first()

        assert(area.isArea)
      }

      it("should treat non-area tags as a line") {
        val line = withArea.where('tags.getField("highway") === "motorway").first()

        assert(!line.isArea)
      }
    }

    describe("withGeometry") {
      implicit val nodes: Dataset[Node with Timestamp] with History = history.nodes

      val geoms: Dataset[OSMFeature with GeometryChanged with MinorVersion with Tags with Validity]
        with History = ways.withGeometry.cache
      val geom = geoms.first()

      it("should include Geometry") {
        assert(geoms.schema.fieldNames.contains("geom"))

        assert(geom.isInstanceOf[Geometry])
      }

      it("should include VersionControl") {
        assert(geoms.schema.fieldNames.contains("changeset"))

        assert(geom.isInstanceOf[VersionControl])
      }

      it("should include Validity") {
        assert(geoms.schema.fieldNames.contains("updated"))
        assert(geoms.schema.fieldNames.contains("validUntil"))

        assert(geom.isInstanceOf[Validity])
      }

      it("should not include Timestamp") {
        assert(!geoms.schema.fieldNames.contains("timestamp"))

        assert(!geom.isInstanceOf[traits.Timestamp])
      }

      it("should produce the correct number of results") {
        // calculated by the previous version (ProcessOSM.reconstructWayGeometry)
        // the difference here is that geometries will actually vary when underlying nodes changed, as they're now
        // captured as doubles (ProcessOSM downsampled them to floats but compared versions as BigDecimals)
        assert(geoms.count === 78836)
      }

      it("should be distinct by changeset") {
        assert(geoms.count === geoms.groupBy('id, 'changeset).agg(first('id)).count)
      }
    }
  }
}
