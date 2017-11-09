package osmesa.ingest.relational

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._


object Columns {
  val NODES = Bytes.toBytes("n")
  val ID = Bytes.toBytes("i")
  val LAT = Bytes.toBytes("t")
  val LON = Bytes.toBytes("g")
  val USER = Bytes.toBytes("u")
  val UID = Bytes.toBytes("d")
  val CHANGESET = Bytes.toBytes("c")
  val VERSION = Bytes.toBytes("v")
  val TIMESTAMP = Bytes.toBytes("t")
  val VISIBLE = Bytes.toBytes("s")
  val MEMBERS = Bytes.toBytes("m")
}
