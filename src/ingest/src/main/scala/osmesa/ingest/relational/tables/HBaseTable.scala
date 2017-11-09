package osmesa.ingest.relational.tables

import org.apache.hadoop.hbase._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._
import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.sql._

import scala.collection.mutable.WrappedArray
import java.nio._


case class HBaseTable(name: String, cfs: List[String])

object HBaseTable {
  implicit class TableMethods(tbl: HBaseTable) {
    def hbaseTableName = TableName.valueOf(tbl.name)

    def descriptor = {
      val tableDefinition = new HTableDescriptor(hbaseTableName)
      tbl.cfs.foreach({ cfamily =>
        tableDefinition.addFamily(new HColumnDescriptor(Bytes.toBytes(cfamily)))
      })
      tableDefinition
    }

    def create(conn: Connection) = {
      val hbAdmin = conn.getAdmin
      try {
        if (! hbAdmin.tableExists(hbaseTableName)) hbAdmin.createTable(descriptor)
      } finally {
        hbAdmin.close()
      }
    }

    def drop(conn: Connection) = {
      val hbAdmin = conn.getAdmin
      try {
        if (hbAdmin.tableExists(hbaseTableName)) hbAdmin.deleteTable(hbaseTableName)
      } finally {
        hbAdmin.close()
      }
    }
  }
}

