package osmesa.query.relational.tables

import osmesa.common.hbase.HBaseTable


object StatTables {
  val hashtags = HBaseTable("hashtags", List("h"))
  val campaign = HBaseTable("campaigns", List("c"))
  val users = HBaseTable("users", List("u"))
}

