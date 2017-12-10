package osmesa.analytics.oneoffs

import osmesa.common.model._
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

    val historyO = Opts.option[String]("history", help = "Location of the History ORC file to process.")
    val changesetsO = Opts.option[String]("changesets", help = "Location of the Changesets ORC file to process.")
    val bucketO = Opts.option[String]("bucket", help = "Bucket to write results to")
    val prefixO = Opts.option[String]("prefix", help = "Prefix of keys path for results.")
    val hashtagsO = Opts.option[String]("hashtags", help = "Comm separated list of hashtags to consider.").orNone

    (
      historyO,
      changesetsO,
      bucketO,
      prefixO,
      hashtagsO
    ).mapN { (historyUri, changesetsUri, bucket, prefix, hashtagsOpt) =>
      val hashtags = hashtagsOpt.map(_.split(",").map(_.toLowerCase).toSet)
      assert(hashtags.size > 0)

      StatsJob.run(historyUri, changesetsUri, bucket, prefix, hashtags)
    }
  }
)

object StatsJob {
  def run(
    historyUri: String,
    changesetsUri: String,
    bucket: String,
    prefix: String,
    hashtagsOpt: Option[Set[String]]
  ): Unit = {
    implicit val spark = Analytics.sparkSession("StatsJob")
    import spark.implicits._

    try {
      val history = spark.read.orc(historyUri)
      val changesets = spark.read.orc(changesetsUri)

      // Filter changesets and history by the target hashtags.

      val filteredChangesets =
        hashtagsOpt match {
          case Some(hashtags) =>
            changesets
              .where(containsHashtags($"tags", hashtags))
          case None =>
            changesets
        }

      val options =
        CalculateStats.Options(
          changesetPartitionCount = 1000,
          wayPartitionCount = 1000,
          nodePartitionCount = 10000
        )

      val (userStats, hashtagStats) =
        CalculateStats.compute(history, filteredChangesets, options)

      val userToPath: User => String = { user =>
        val p = new java.io.File(prefix, "users").getPath
        s"${p}/${user.uid}.json"
      }

      val hashtagToPath: Campaign => String = { hashtag =>
        val p = new java.io.File(prefix, "hashtag").getPath
        val id = java.net.URLEncoder.encode(hashtag.tag, "UTF-8")
        s"${p}/${id}.json"
      }

      // Write JSON to correct location
      JsonRDDWriter.
        write(
          userStats.map(_.toCoreType),
          bucket,
          userToPath
        )

      JsonRDDWriter.
        write(
          hashtagStats.map(_.toCoreType),
          bucket,
          hashtagToPath
        )
    } finally {
      spark.stop()
    }
  }
}
