package osmesa.analytics

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Analytics {
  def sparkSession(appName: String): SparkSession = {
    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setAppName(s"OSMesa Analytics - ${appName}")
      .set("spark.sql.orc.filterPushdown", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)

    SparkSession.builder
        .config(conf)
        .enableHiveSupport
        .getOrCreate
  }
}
