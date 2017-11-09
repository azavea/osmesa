package osmesa.ingest.relational

import osmesa.ingest.relational.tables._
import osmesa.ingest.util._

import geotrellis.util.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util._
import org.apache.spark.sql._


object OsmFeatureIngest extends SparkProcess with LazyLogging {
  def main(args: Array[String]) = {
    val orcUri = args(0)
    val iom = ss.read.orc(orcUri)
    val conn = HBase.getConn()
    val testMutator = conn.getBufferedMutator(OsmTables.nodes.hbaseTableName)
    val testSize = testMutator.getWriteBufferSize()
    println(s"Write buffer set to: ($testSize)")
    try {
      OsmTables.nodes.create(conn)
      OsmTables.ways.create(conn)
      OsmTables.relations.create(conn)
    } finally {
      testMutator.close()
      conn.close()
    }

    iom.foreachPartition({ partIter =>
      val conn = HBase.getConn()
      // Mutators are the preferred method for writing batch puts (which perform better for consistency and speed on S3)
      val nodeMutator = conn.getBufferedMutator(OsmTables.nodes.hbaseTableName)
      val wayMutator = conn.getBufferedMutator(OsmTables.ways.hbaseTableName)
      val relMutator = conn.getBufferedMutator(OsmTables.relations.hbaseTableName)
      try {
        if (! partIter.isEmpty) partIter.foreach({ feature =>
          try {
            feature.getString(1) match {
              case "node" => OsmNodeIngest(feature, nodeMutator)
              case "way" => OsmWayIngest(feature, wayMutator)
              case "relation" => OsmRelIngest(feature, relMutator)
              case t if (t.size > 0) =>
                logger.warn(s"Unexpected type encountered while ingesting OSM feature: [type: $t, row: $feature]")
              case t =>
                logger.warn(s"OSM feature lacks type information. [row: $feature]")
            }
          } catch {
            case ia: java.lang.IllegalArgumentException => println(s"THE FAILING FEATURE IS: ($feature)")
          }
        })
      } finally {
        nodeMutator.close()
        wayMutator.close()
        relMutator.close()
        conn.close()
      }
    })
  }
}

