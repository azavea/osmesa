package osmesa.analytics

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Analytics {
  def sparkSession(appName: String): SparkSession = {
    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setAppName(s"OSMesa Analytics - ${appName}")
      .set("spark.sql.orc.filterPushdown", "true")
      .set("spark.hadoop.parquet.enable.summary-metadata", "false")
      .set("spark.sql.parquet.mergeSchema", "false")
      .set("spark.sql.parquet.filterPushdown", "true")
      .set("spark.sql.hive.metastorePartitionPruning", "true")
    // Commenting out the kryo serialization, as it was causing stack overflow exceptions
    // for the vector tile generation jobs.
      // .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .set("spark.kryo.registrator", classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)

    SparkSession.builder
        .config(conf)
        .enableHiveSupport
        .getOrCreate
  }

  /** Returns a DataFrame of two columns:
    * | changeset ID | hashtag |
    * With a row for every hashtag that the changeset participates in (without # and lowercase)
    */
  def changesetToHashtag(changesets: DataFrame)(implicit ss: SparkSession): DataFrame = {
    import ss.implicits._

    changesets
      .where($"tags".getItem("comment").contains("#"))
      .withColumn("hashtags", hashtags($"tags"))
      .where(size($"hashtags") > 0)
      .select($"id", explode($"hashtags").as("hashtag"))
  }


}
