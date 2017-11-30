package osmesa.ingest.relational

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._


object CampaignColumns {
  val TAG = Bytes.toBytes("t")
  val ROADADD = Bytes.toBytes("ra")
  val ROADMOD = Bytes.toBytes("rm")
  val BUILDINGADD = Bytes.toBytes("ba")
  val BUILDINGMOD = Bytes.toBytes("bm")
  val WATERWAYADD = Bytes.toBytes("wa")
  val POIADD = Bytes.toBytes("pa")
  val KMROADADD = Bytes.toBytes("kra")
  val KMROADMOD = Bytes.toBytes("krm")
  val WATERWAYKMADD = Bytes.toBytes("kwa")
}

