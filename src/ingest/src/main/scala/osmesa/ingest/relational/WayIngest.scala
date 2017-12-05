package osmesa.ingest.relational

import osmesa.ingest.relational.tables._
import osmesa.common.proto
import osmesa.ingest.util._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, BufferedMutator}
import org.apache.hadoop.hbase.util._
import org.apache.spark.sql._

import scala.collection.mutable.WrappedArray
import java.nio._


object WayIngest {
  def apply(way: Row, mutator: BufferedMutator): Unit = {
    val id = way.getLong(0)
    val tags = way.getAs[Map[String, String]](2)
    val nodes = {
      val ns: Seq[Long] = way.getAs[Seq[Row]](5).map({ case Row(l: Long) => l }).toSeq
      proto.LongList(ns).toByteArray
    }
    val changeset = way.getLong(7)
    val timestamp = way.getTimestamp(8).toInstant.toEpochMilli
    val uid = way.getLong(9)
    val user = way.getString(10)
    val version = way.getLong(11)
    val visible = way.getBoolean(12)

    val META_CF = Bytes.toBytes(FeatureTables.ways.cfs(0))
    val TAG_CF = Bytes.toBytes(FeatureTables.ways.cfs(1))

    val put = new Put(Bytes.toBytes(id) ++ Bytes.toBytes(timestamp / 3600000))
    put.addColumn(META_CF, FeatureColumns.ID, Bytes.toBytes(id))
    put.addColumn(META_CF, FeatureColumns.NODES, nodes)
    put.addColumn(META_CF, FeatureColumns.USER, Bytes.toBytes(user))
    put.addColumn(META_CF, FeatureColumns.UID, Bytes.toBytes(uid))
    put.addColumn(META_CF, FeatureColumns.VERSION, Bytes.toBytes(version))
    put.addColumn(META_CF, FeatureColumns.TIMESTAMP, Bytes.toBytes(timestamp))
    put.addColumn(META_CF, FeatureColumns.VISIBLE, Bytes.toBytes(visible))
    tags.foreach({ case (k, v) => put.addColumn(TAG_CF, Bytes.toBytes(k), Bytes.toBytes(v)) })
    mutator.mutate(put)
  }
}

