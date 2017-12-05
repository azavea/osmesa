package osmesa.ingest.relational.tables

import osmesa.common.hbase.HBaseTable

object FeatureTables {
  val nodes = HBaseTable("osm_nodes", List("m", "t"))
  val ways = HBaseTable("osm_ways", List("m", "t"))
  val relations = HBaseTable("osm_rels", List("m", "t"))
}

