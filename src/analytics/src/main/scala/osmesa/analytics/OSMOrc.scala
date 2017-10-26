package osmesa.analytics

import org.apache.spark.sql._

object OSMOrc {
  def planetHistory(implicit ss: SparkSession): DataFrame =
    ss.read.orc("s3://osm-pds/planet-history/history-latest.orc")

  def planetLatest(implicit ss: SparkSession): DataFrame =
    ss.read.orc("s3://osm-pds/planet/planet-latest.orc")

  def changesets(implicit ss: SparkSession): DataFrame =
    ss.read.orc("s3://osm-pds/changesets/changesets-latest.orc")
}
