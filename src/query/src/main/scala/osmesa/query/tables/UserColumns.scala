package osmesa.ingest.relational

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._


object UserColumns {
  val UID = Bytes.toBytes("id")
  val NAME = Bytes.toBytes("n")
  val GEO_EXTENT = Bytes.toBytes("ge")
  val BUILDING_ADD = Bytes.toBytes("ba")
  val BUILDING_MOD = Bytes.toBytes("bm")
  val POI_ADD = Bytes.toBytes("pa")
  val KM_WATERWAY_ADD = Bytes.toBytes("kwa")
  val WATERWAY_ADD = Bytes.toBytes("wa")
  val KM_ROAD_ADD = Bytes.toBytes("kra")
  val KM_ROAD_MOD = Bytes.toBytes("krm")
  val ROAD_ADD = Bytes.toBytes("ra")
  val ROAD_MOD = Bytes.toBytes("rm")
  val CHANGESET = Bytes.toBytes("chg")
  val EDIT_COUNT = Bytes.toBytes("ec")
  val EDIT_TIMES = Bytes.toBytes("et")
  val COUNTRY_LIST = Bytes.toBytes("cl")
  val HASHTAG = Bytes.toBytes("ht")
}

