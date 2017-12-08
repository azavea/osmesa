package osmesa.stats.util

import com.monovore.decline._
import osmesa.stats.relational.tables._
import osmesa.stats.util._

import geotrellis.util.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, BufferedMutator}
import org.apache.hadoop.hbase.util._
import org.apache.spark.sql._


object MockIngest extends CommandApp(
  name = "Ingest random statistics values to HBase for testing purposes",
  header = "Generate test data for OSMesa",
  main = {
    val userO = Opts.option[Int]("users", short = "u", metavar = "count", help = "Set a number of (generated) users to ingest.")
    val campaignO = Opts.option[Int]("campaign", short = "c", metavar = "count", help = "Set a number of (generated) campaigns to ingest.")

    (userO, campaignO).mapN({ (userCount, campaignCount) =>
      (1 to userCount).foreach({ _ =>
        val user = User.random
        val uid = Bytes.toBytes(user.uid)
        val name = Bytes.toBytes(user.name)
        val geoExtent = Bytes.toBytes(user.geoExtent)
        val buildingCountAdd = Bytes.toBytes(user)
        val buildingCountMod = Bytes.toBytes(user)
        val poiCountAdd = Bytes.toBytes(user)
        val waterwayKmAdd = Bytes.toBytes(user)
        val roadKmAdd = Bytes.toBytes(user)
        val roadKmMod = Bytes.toBytes(user)
        val roadCountAdd = Bytes.toBytes(user)
        val roadCountMod = Bytes.toBytes(user)
        val changesetCount = Bytes.toBytes(user)
        val editCount = Bytes.toBytes(user)
        val editTimes = proto.LongList(user.editTimes.map(_.toEpochMilli)).toByteArray
        val countryList = proto.Countries(user.countryList).toByteArray
        val hashtags = proto.Hashtags(user.hashtags).toByteArray

      })
    })
  }
)

object NodeIngest {
  def apply(node: Row, mutator: BufferedMutator): Unit = {
    val id = node.getLong(0)
    val tags = node.getAs[Map[String, String]](2)
    val lat = Option(node.getDecimal(3)).map(_.doubleValue).getOrElse(Double.NaN)
    val lon = Option(node.getDecimal(4)).map(_.doubleValue).getOrElse(Double.NaN)
    val changeset = node.getLong(7)
    val timestamp = node.getTimestamp(8).toInstant.toEpochMilli
    val uid = node.getLong(9)
    val user = node.getString(10)
    val version = node.getLong(11)
    val visible = node.getBoolean(12)

    val META_CF = Bytes.toBytes(FeatureTables.nodes.cfs(0))
    val TAG_CF = Bytes.toBytes(FeatureTables.nodes.cfs(1))

    val put = new Put(Bytes.toBytes(id) ++ Bytes.toBytes(timestamp / 3600000))
    put.addColumn(META_CF, Columns.ID, Bytes.toBytes(id))
    put.addColumn(META_CF, Columns.LAT, Bytes.toBytes(lat))
    put.addColumn(META_CF, Columns.LON, Bytes.toBytes(lon))
    put.addColumn(META_CF, Columns.USER, Bytes.toBytes(user))
    put.addColumn(META_CF, Columns.UID, Bytes.toBytes(uid))
    put.addColumn(META_CF, Columns.VERSION, Bytes.toBytes(version))
    put.addColumn(META_CF, Columns.TIMESTAMP, Bytes.toBytes(timestamp))
    put.addColumn(META_CF, Columns.VISIBLE, Bytes.toBytes(visible))
    tags.foreach({ case (k, v) => put.addColumn(TAG_CF, Bytes.toBytes(k), Bytes.toBytes(v)) })
    mutator.mutate(put)
  }
}
