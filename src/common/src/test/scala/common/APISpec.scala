package common

import java.math.BigDecimal
import java.sql.Timestamp

import geotrellis.vector.Extent
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.scalatest.FunSpec
import osmesa.common.{TestEnvironment, _}

class APISpec extends FunSpec with TestEnvironment {
  import ss.implicits._

  val ChangesetsFile: String = getClass.getResource("/changesets.orc").getPath
  val ChangesetsDF: DataFrame = ss.read.orc(ChangesetsFile)
  val HistoryFile: String = getClass.getResource("/disneyland.osh.orc").getPath
  val HistoryDF: DataFrame = ss.read.orc(HistoryFile)
  val SnapshotFile: String = getClass.getResource("/isle-of-man-latest.osm.orc").getPath
  val SnapshotDF: DataFrame = ss.read.orc(SnapshotFile)

  describe("asChangesets") {
    val df = ChangesetsDF
    val ds = asChangesets(df)

    it("should contain the same fields") {
      assert(df.schema.fieldNames.length === ds.schema.fieldNames.length)
    }

    it("should fail with partial data") {
      assertThrows[AnalysisException] {
        asChangesets(df.select('id, 'created_at))
      }
    }

    it("should pass data through unmodified (except for types)") {
      val conditions = 'id === 983634

      val c1 = df.where(conditions).first()
      val c2 = ds.where(conditions).first()

      assert(c1.getAs[Long]("id") === c2.id)
      assert(c1.getAs[Map[String, String]]("tags") === c2.tags)
      assert(c1.getAs[Timestamp]("created_at") === c2.createdAt)
      assert(Some(c1.getAs[Timestamp]("closed_at")) === c2.closedAt)
      assert(c1.getAs[Boolean]("open") === c2.open)
      assert(c1.getAs[Long]("uid") === c2.uid)
      assert(c1.getAs[String]("user") === c2.user)

      assert(Option(c1.getAs[BigDecimal]("min_lat")).map(_.floatValue()) === c2.minLat)
      assert(Option(c1.getAs[BigDecimal]("max_lat")).map(_.floatValue()) === c2.maxLat)
      assert(Option(c1.getAs[BigDecimal]("min_lon")).map(_.floatValue()) === c2.minLon)
      assert(Option(c1.getAs[BigDecimal]("max_lon")).map(_.floatValue()) === c2.maxLon)

      assert(c1.getAs[Int]("num_changes") === c2.numChanges)
      assert(c1.getAs[Int]("comments_count") === c2.commentsCount)
    }
  }

  describe("asHistory") {
    val df = HistoryDF
    val ds = asHistory(df)

    it("should contain the same fields") {
      assert(df.schema.fieldNames === ds.schema.fieldNames)
    }

    it("should fail with partial data") {
      assertThrows[AnalysisException] {
        asHistory(df.select('id, 'type, 'tags, 'lat, 'lon))
      }
    }

    it("should include additional columns") {
      val ds = asHistory(df.withColumn("something", lit("something")))

      assert(ds.schema.fieldNames.contains("something"))

      val row = ds.select('something).first()

      assert(row.getAs[String]("something") === "something")
    }

    it("should coerce type to a Byte") {
      val typeType = ds.schema.fields(ds.schema.fieldIndex("type"))
      assert(typeType.dataType.isInstanceOf[ByteType])
    }

    it("should compress relation members") {
      import osmesa.common.implicits._

      val el = ds.relations.where('id === 83689 and 'version === 1).first()

      assert(el.members.head.`type` === WayType)
    }

    it("should pass node data through unmodified (except for types)") {
      val conditions = 'id === 14840391

      val node1 = df.where('type === "node" and conditions).first()
      val node2 = ds.where('type === NodeType and conditions).first()

      assert(node1.getAs[Long]("id") === node2.id)
      assert(NodeType === node2.`type`)
      assert(node1.getAs[Map[String, String]]("tags") === node2.tags)
      assert(Option(node1.getAs[BigDecimal]("lat")).map(_.floatValue()) === node2.lat)
      assert(Option(node1.getAs[BigDecimal]("lon")).map(_.floatValue()) === node2.lon)
      assert(node1.getAs[Long]("changeset") === node2.changeset)
      assert(node1.getAs[Timestamp]("timestamp") === node2.timestamp)
      assert(node1.getAs[Long]("uid") === node2.uid)
      assert(node1.getAs[String]("user") === node2.user)
      assert(node1.getAs[Int]("version") === node2.version)
      assert(node1.getAs[Boolean]("visible") === node2.visible)
    }

    it("should treat deleted node coordinates as None") {
      val deletedNode = ds.where('id === 14689244 and 'version === 22).first()

      assert(!deletedNode.visible)
      assert(deletedNode.lat === None)
      assert(deletedNode.lon === None)
    }

    it("should pass way nodes through") {
      val conditions = 'id === 4004143 and 'version === 19

      val way1 = df.where('type === "way" and conditions).first()
      val way2 = ds.where('type === WayType and conditions).first()

      val dfNds = way1.getSeq[Row](way1.fieldIndex("nds")).map(_.getAs[Long]("ref"))

      assert(dfNds === way2.nds)
    }

    it("should pass relation members through") {
      val conditions = 'id === 83689 and 'version === 1

      val relation1 = df.where('type === "relation" and conditions).first()
      val relation2 = ds.where('type === RelationType and conditions).first()

      val dfMembers = relation1
        .getSeq[Row](relation1.fieldIndex("members"))
        .map(row =>
          (row.getAs[String]("type"): Byte, row.getAs[Long]("ref"), row.getAs[String]("role")))

      val historyMembers = relation2.members.map(m => (m.`type`, m.ref, m.role))

      assert(dfMembers === historyMembers)
    }
  }

  describe("asSnapshot") {
    val df = SnapshotDF
    val snapshot = asSnapshot(df)
    val ds = snapshot.dataset

    it("should contain the same fields") {
      assert(df.schema.fieldNames === ds.schema.fieldNames)
    }

    it("should fail with partial data") {
      assertThrows[AnalysisException] {
        asSnapshot(df.select('id, 'type, 'tags, 'lat, 'lon))
      }
    }

    it("should include additional columns") {
      val ds = asHistory(df.withColumn("something", lit("something")))

      assert(ds.schema.fieldNames.contains("something"))

      val row = ds.select('something).first()

      assert(row.getAs[String]("something") === "something")
    }

    it("should coerce `type` to a Byte") {
      val typeType = ds.schema.fields(ds.schema.fieldIndex("type"))
      assert(typeType.dataType.isInstanceOf[ByteType])
    }

    it("should compress relation members") {
      import osmesa.common.implicits._

      val el = ds.relations.where('id === 58446 and 'version === 150).first()

      assert(el.members.head.`type` === NodeType)
    }
  }

  describe("Dataset[OSM] with History") {
    import implicits._

    val history = asHistory(HistoryDF)
    val timestamp = Timestamp.valueOf("2012-01-01 00:00:00")
    val nodes = history.nodes

    describe("nodes") {
      it("should produce only nodes") {
        assert(nodes.count === 290861)
      }

      it("should include Tags") {
        assert(nodes.schema.fieldNames.contains("tags"))
      }
    }

    describe("ways") {
      it("should produce only ways") {
        assert(history.ways.count === 75849)
      }
    }

    describe("relations") {
      it("should produce only relations") {
        assert(history.relations.count === 4825)
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
      val ds = history.withValidity

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

  describe("Dataset[Node with Timestamp]") {
    import implicits._

    val nodes = asHistory(HistoryDF).nodes

    describe("asPoints") {
      val geoms = nodes.asPoints.cache

      it("should include Geometry") {
        assert(geoms.schema.fieldNames.contains("geom"))
      }

      it("should include Metadata") {
        assert(geoms.schema.fieldNames.contains("changeset"))
        assert(geoms.schema.fieldNames.contains("uid"))
        assert(geoms.schema.fieldNames.contains("user"))
      }

      it("should include Timestamp") {
        assert(geoms.schema.fieldNames.contains("timestamp"))
      }

      it("should produce the correct number of results") {
        assert(geoms.count === 30487)
      }

      it("should be distinct by changeset") {
        assert(geoms.count === geoms.groupBy('id, 'changeset).agg(first('id)).count)
      }
    }
  }

  describe("Dataset[Node]") {
    import implicits._

    val nodes = asHistory(HistoryDF).nodes.withValidity

    describe("asPoints") {
      val geoms = nodes.asPoints.cache

      it("should include Geometry") {
        assert(geoms.schema.fieldNames.contains("geom"))
      }

      it("should include Metadata") {
        assert(geoms.schema.fieldNames.contains("changeset"))
        assert(geoms.schema.fieldNames.contains("uid"))
        assert(geoms.schema.fieldNames.contains("user"))
      }

      it("should not include Timestamp") {
        assert(!geoms.schema.fieldNames.contains("timestamp"))
      }

      it("should produce the correct number of results") {
        assert(geoms.count === 33533)
      }

      it("should be distinct by changeset") {
        assert(geoms.count === geoms.groupBy('id, 'changeset).agg(first('id)).count)
      }
    }
  }

  describe("Dataset[Way with Timestamp] with History") {
    import implicits._

    val ways = asHistory(HistoryDF).ways

    describe("withValidity") {
      val ds = ways.withValidity

      it("should exclude `type`") {
        assert(!ds.schema.fieldNames.contains("type"))
      }

      it("should exclude coordinates") {
        assert(!ds.schema.fieldNames.contains("lat"))
        assert(!ds.schema.fieldNames.contains("lon"))
      }

      it("should exclude `members`") {
        assert(!ds.schema.fieldNames.contains("members"))
      }

      it("should set validUntil") {
        val el = ds.where('id === 49824922 and 'version === 1).first()

        assert(el.validUntil === Some(Timestamp.valueOf("2010-02-07 17:00:33")))
      }

      it("should set geometryChanged") {
        pending
      }
    }
  }

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

  describe("Dataset[Coordinates]") {
    import implicits._

    val nodes = asHistory(HistoryDF).nodes

    describe("filter(Extent)") {
      val extent = Extent(-117.923083, 33.811298, -117.920122, 33.814035)
      val ds = nodes.filter(extent)

      it("should filter nodes") {
        assert(ds.count === 13700)
      }
    }
  }

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
