import osmesa.analytics.stats._

import osmesa.analytics._

object TEMP {
  def main(args: Array[String]): Unit = {
    implicit val spark = Analytics.sparkSession("boo")
    import spark.implicits._

    val changesets = spark.read.orc("/Users/rob/Downloads/ri-changesets.orc").repartition(200, $"id")
    val history = spark.read.orc("/Users/rob/Downloads/rhode-island.osh.orc").repartition(200, $"changeset")

    val changesetStats = CalculateStats.computeChangesetStats(history, changesets, CalculateStats.Options(200, 200, 200))

    changesetStats.count()
  }
}
