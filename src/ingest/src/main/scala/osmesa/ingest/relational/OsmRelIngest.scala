package osmesa.ingest.relational

import osmesa.ingest.relational.tables._
import osmesa.common.proto
import osmesa.ingest.util._

import vectorpipe.osm.Member
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, BufferedMutator}
import org.apache.hadoop.hbase.util._
import org.apache.spark.sql._


object OsmRelIngest {
  def apply(rel: Row, mutator: BufferedMutator): Unit = {
    val id = rel.getLong(0)
    val tags = rel.getAs[Map[String, String]](2)
    val members: Array[Byte] = {
      val memberSeq = rel.getAs[Seq[Row]](6).map({ case Row(t: String, ref: Long, role: String) =>
        proto.Members.Member(t, ref, role)
      }).toSeq
      proto.Members(memberSeq).toByteArray
    }
    val changeset = rel.getLong(7)
    val timestamp = rel.getTimestamp(8).toInstant.toEpochMilli
    val uid = rel.getLong(9)
    val user = rel.getString(10)
    val version = rel.getLong(11)
    val visible = rel.getBoolean(12)

    val META_CF = Bytes.toBytes(OsmTables.relations.cfs(0))
    val TAG_CF = Bytes.toBytes(OsmTables.relations.cfs(1))

    val put = new Put(Bytes.toBytes(id) ++ Bytes.toBytes(timestamp / 3600000))
    put.addColumn(META_CF, Columns.ID, Bytes.toBytes(id))
    put.addColumn(META_CF, Columns.MEMBERS, members)
    put.addColumn(META_CF, Columns.USER, Bytes.toBytes(user))
    put.addColumn(META_CF, Columns.UID, Bytes.toBytes(uid))
    put.addColumn(META_CF, Columns.VERSION, Bytes.toBytes(version))
    put.addColumn(META_CF, Columns.TIMESTAMP, Bytes.toBytes(timestamp))
    put.addColumn(META_CF, Columns.VISIBLE, Bytes.toBytes(visible))
    tags.foreach({ case (k, v) => put.addColumn(TAG_CF, Bytes.toBytes(k), Bytes.toBytes(v)) })
    mutator.mutate(put)
  }
}

