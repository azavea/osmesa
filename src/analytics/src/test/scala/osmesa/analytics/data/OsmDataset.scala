package osmesa.analytics.data

import osmesa.analytics._
import osmesa.analytics.stats._

import org.apache.spark.sql._

trait OsmDataset {
  def history(implicit ss: SparkSession): DataFrame
  def changesets(implicit ss: SparkSession): DataFrame
  def stats: (Seq[UserStats], Seq[HashtagStats])
}

object OsmDataset {
  def build(f: (TestData.Elements, TestData.Changes) => Unit): OsmDataset = {
    val elements = new TestData.Elements
    val changes = new TestData.Changes
    f(elements, changes)
    new OsmDataset {
      def history(implicit ss: SparkSession) = TestData.createHistory(changes.historyRows)
      def changesets(implicit ss: SparkSession) = TestData.createChangesets(changes.changesetRows)
      def stats = changes.stats
    }
  }
}
