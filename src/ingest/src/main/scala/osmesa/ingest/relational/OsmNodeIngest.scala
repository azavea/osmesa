package osmesa.ingest.relational

import osmesa.ingest.relational.tables._
import osmesa.ingest.util._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, BufferedMutator}
import org.apache.hadoop.hbase.util._
import org.apache.spark.sql._


object OsmNodeIngest {
  def apply(node: Row, mutator: BufferedMutator): Unit = {
    val id = node.getLong(0)
    val tags = node.getAs[Map[String, String]](2)
    val lat = Option(node.getDecimal(3)).map(_.doubleValue).getOrElse(Double.NaN)
    val lon = Option(node.getDecimal(4)).map(_.doubleValue).getOrElse(Double.NaN)
    val changeset = node.getLong(7)
    val timestamp = node.getTimestamp(8).toInstant.toEpochMilli
    val uid = node.getLong(9)
    val user = node.getString(10)
    val version = node.getLong(11)
    val visible = node.getBoolean(12)

    val META_CF = Bytes.toBytes(OsmTables.nodes.cfs(0))
    val TAG_CF = Bytes.toBytes(OsmTables.nodes.cfs(1))

    val put = new Put(Bytes.toBytes(id) ++ Bytes.toBytes(timestamp / 3600000))
    put.addColumn(META_CF, Columns.ID, Bytes.toBytes(id))
    put.addColumn(META_CF, Columns.LAT, Bytes.toBytes(lat))
    put.addColumn(META_CF, Columns.LON, Bytes.toBytes(lon))
    put.addColumn(META_CF, Columns.USER, Bytes.toBytes(user))
    put.addColumn(META_CF, Columns.UID, Bytes.toBytes(uid))
    put.addColumn(META_CF, Columns.VERSION, Bytes.toBytes(version))
    put.addColumn(META_CF, Columns.TIMESTAMP, Bytes.toBytes(timestamp))
    put.addColumn(META_CF, Columns.VISIBLE, Bytes.toBytes(visible))
    tags.foreach({ case (k, v) => put.addColumn(TAG_CF, Bytes.toBytes(k), Bytes.toBytes(v)) })
    mutator.mutate(put)
  }
}

