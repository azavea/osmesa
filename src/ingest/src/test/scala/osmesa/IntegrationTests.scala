package osmesa

import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes

import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Put

import scala.collection.JavaConverters._

object IntegrationTests {
  def main(args: Array[String]): Unit = {
    // Instantiating Configuration class
    val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", "zookeeper")
    println("1")

    // Instantiating HTable class
    val table = new HTable(config, "test")
    println(s"TABLE: $table")

    table.getRegionLocator.getAllRegionLocations.asScala.foreach { l =>
      println(s"Region Location: $l")
    }

    val CF = "cf".getBytes
    val ATTR = "a".getBytes

    val put = new Put(Bytes.toBytes("row-api-1"))
    put.add(CF, ATTR, Bytes.toBytes("value-api-1"))
    table.put(put)

    // Instantiating the Scan class
    val scan = new Scan()
    println("3")

    // Getting the scan result
    val scanner = table.getScanner(scan)
    println("4")

    // Reading values from scan result
    var result: Result = scanner.next
    if(result == null) { println("NO RESULTS!") }
    while(result != null) {
      println(s"Found row : {result}")
      result = scanner.next()
    }

    //closing the scanner
    scanner.close()
  }
}
