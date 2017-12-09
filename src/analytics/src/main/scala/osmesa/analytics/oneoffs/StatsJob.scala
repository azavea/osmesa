package osmesa.analytics.oneoffs

import osmesa.analytics._
import osmesa.analytics.stats._

import scala.util.{Try, Success, Failure}

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql._

object StatsJobCommand extends CommandApp(

  name   = "calc-stats",
  header = "Calculate statistics",
  main   = {

    val historyO = Opts.option[String]("orc", help = "Location of the History ORC file to process.")
    val changesetsO = Opts.option[String]("orc", help = "Location of the Changesets ORC file to process.")
    val hashtagsO = Opts.option[String]("orc", help = "Comm separated list of hashtags to consider.")

    (historyO, changesetsO, hashtagsO).mapN { (historyUri, changesetsUri, hashtagsStr) =>
      val hashtags = hashtagsStr.split(",").map(_.toLowerCase).toSet
      assert(hashtags.size > 0)

      StatsJob.run(historyUri, changesetsUri, hashtags)
    }
  }
)

object StatsJob {
  def run(historyUri: String, changesetsUri: String, hashtags: Set[String]): Unit = {
    implicit val spark = Analytics.sparkSession("StatsJob")
    import spark.implicits._

    try {
      val history = spark.read.orc(historyUri)
      val changesets = spark.read.orc(changesetsUri)

      // Filter changesets and history by the target hashtags.

      val filteredChangesets =
        changesets
          .where(containsHashtags($"tags", hashtags))

      val targetChangesetIds = filteredChangesets.select($"id".as("changeset"))

      val options =
        CalculateStats.Options(
          changesetPartitionCount = 1000,
          wayPartitionCount = 1000,
          nodePartitionCount = 10000
        )

      val (userStats, hashtagStats) =
        CalculateStats.compute(history, filteredChangesets, options)

      // Write JSON to correct location
      ???

    } finally {
      spark.stop()
    }
  }
}
