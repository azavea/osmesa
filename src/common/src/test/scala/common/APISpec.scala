package common

import java.math.BigDecimal
import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.{AnalysisException, Row}
import org.scalatest.FunSpec
import osmesa.common._

class APISpec extends FunSpec with TestEnvironment {
  import ss.implicits._

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

      assert(Option(c1.getAs[BigDecimal]("min_lat")).map(_.doubleValue()) === c2.minLat)
      assert(Option(c1.getAs[BigDecimal]("max_lat")).map(_.doubleValue()) === c2.maxLat)
      assert(Option(c1.getAs[BigDecimal]("min_lon")).map(_.doubleValue()) === c2.minLon)
      assert(Option(c1.getAs[BigDecimal]("max_lon")).map(_.doubleValue()) === c2.maxLon)

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
      val conditions = 'id === 14840391 and 'version === 2

      val node1 = df.where('type === "node" and conditions).first()
      val node2 = ds.where('type === NodeType and conditions).first()

      assert(node1.getAs[Long]("id") === node2.id)
      assert(NodeType === node2.`type`)
      assert(node1.getAs[Map[String, String]]("tags") === node2.tags)
      assert(Option(node1.getAs[BigDecimal]("lat")).map(_.doubleValue()) === node2.lat)
      assert(Option(node1.getAs[BigDecimal]("lon")).map(_.doubleValue()) === node2.lon)
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
}
