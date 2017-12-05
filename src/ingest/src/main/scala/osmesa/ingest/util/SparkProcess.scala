package osmesa.ingest.util

import org.apache.spark._
import org.apache.spark.sql._


trait SparkProcess {
  /* Settings compatible for both local and EMR execution */
  val sc = new SparkConf()
    .setIfMissing("spark.master", "local[*]")
    .setAppName("vp-orc-io")

  implicit val ss: SparkSession = SparkSession.builder
    .config(sc)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
    .enableHiveSupport
    .getOrCreate
}

