package osmesa.apps.batch

import java.net.URI

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.max
import osmesa.analytics.Analytics
import vectorpipe.model.AugmentedDiff
import vectorpipe.sources.{AugmentedDiffSource, Source}

object ChangesetComparator extends CommandApp(
  name = "changeset-comparator",
  header = "Changeset Comparator",
  main = {
    val historyOrcUriOpt =
      Opts.argument[URI](metavar = "historyOrcUri")

    val augmentedDiffUriOpt = Opts.argument[URI](metavar = "augmentedDiffUri")

    val outputUriOpt = Opts.argument[URI](metavar = "outputUri")

    (historyOrcUriOpt, augmentedDiffUriOpt, outputUriOpt).mapN { (historyOrcUri, augmentedDiffUri, outputUri) =>
      implicit val spark: SparkSession = Analytics.sparkSession("ChangesetComparator")
      import spark.implicits._

      val history: DataFrame = spark.read.orc(historyOrcUri.toString)

      val startSequence = {
        //        val t = history.select(min('timestamp)).first.getAs[java.sql.Timestamp](0)
        //        AugmentedDiffSource.timestampToSequence(t)
        //        1783379 // First sequence id in planet osm
        3200724   // First sequence id in aug diff stream
      }
      val endSequence = {
        val t = history.select(max('timestamp)).first.getAs[java.sql.Timestamp](0)
        AugmentedDiffSource.timestampToSequence(t)
//        3899869   // Last sequence id in planet osm
      }

      print(s"Comparing history to augmented diffs from $startSequence to $endSequence...")

      val options = Map(
        Source.BaseURI -> augmentedDiffUri.toString,
        Source.StartSequence -> startSequence.toString,
        Source.EndSequence -> endSequence.toString
      )
      val augmentedDiffs = spark.read
        .format(Source.AugmentedDiffs).options(options).load.as[AugmentedDiff]

      val historyChangesets = history
        .where('timestamp >= AugmentedDiffSource.sequenceToTimestamp(startSequence))
        .where('timestamp <= AugmentedDiffSource.sequenceToTimestamp(endSequence))
        .select('changeset)
        .withColumnRenamed("changeset", "h_changeset")
        .distinct

      val augDiffChangesets = augmentedDiffs
        .where('sequence >= startSequence)
        .where('sequence <= endSequence)
        .select('changeset)
        .withColumnRenamed("changeset", "a_changeset")
        .distinct

      val result = historyChangesets
        .join(augDiffChangesets, 'h_changeset === 'a_changeset, "full_outer")
        .persist

      val only_history_changeset_count = result.filter('a_changeset.isNull).count
      print(s"Only history changesets: ${only_history_changeset_count}")
      val only_adiff_changeset_count = result.filter('h_changeset.isNull).count
      print(s"Only adiff changesets: ${only_adiff_changeset_count}")

      result.write.format("csv").save(outputUri.toString)
    }
  }
)