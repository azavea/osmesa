package osmesa.ingest.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._


object HBase {
  def getConf() = {
    val conf = HBaseConfiguration.create()
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
    conf.addResource(new Path("/etc/hadoop/conf/hadoop-site.xml"))
    conf
  }

  def getConn() = ConnectionFactory.createConnection(getConf)

  def getAdmin() = getConn.getAdmin()
}
