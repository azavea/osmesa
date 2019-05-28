package osmesa.analytics.examples

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.sql._
import osmesa.analytics.Analytics
import vectorpipe.model.ElementWithSequence
import vectorpipe.sources.Source

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.examples.AugmentedDiffStreamProcessor \
 *   ingest/target/scala-2.11/osmesa-analytics.jar \
 *   --augmented-diff-source s3://somewhere/diffs/
 */
object AugmentedDiffStreamProcessor
    extends CommandApp(
      name = "osmesa-augmented-diff-stream-processor",
      header = "Update statistics from streaming augmented diffs",
      main = {
        type AugmentedDiffFeature = Feature[Geometry, ElementWithSequence]

        val augmentedDiffSourceOpt = Opts.option[URI](
          "augmented-diff-source",
          short = "a",
          metavar = "uri",
          help = "Location of augmented diffs to process"
        )
        val startSequenceOpt = Opts
          .option[Int](
            "start-sequence",
            short = "s",
            metavar = "sequence",
            help = "Starting sequence. If absent, the current (remote) sequence will be used."
          )
          .orNone
        val endSequenceOpt = Opts
          .option[Int](
            "end-sequence",
            short = "e",
            metavar = "sequence",
            help = "Ending sequence. If absent, this will be an infinite stream."
          )
          .orNone

        (augmentedDiffSourceOpt, startSequenceOpt, endSequenceOpt)
          .mapN {
            (augmentedDiffSource, startSequence, endSequence) =>
              implicit val ss: SparkSession =
                Analytics.sparkSession("AugmentedDiffStreamProcessor")

              val options = Map(Source.BaseURI -> augmentedDiffSource.toString,
                                Source.ProcessName -> "AugmentedDiffStreamProcessor") ++
                startSequence
                  .map(s => Map(Source.StartSequence -> s.toString))
                  .getOrElse(Map.empty[String, String]) ++
                endSequence
                  .map(s => Map(Source.EndSequence -> s.toString))
                  .getOrElse(Map.empty[String, String])

              val geoms =
                ss.readStream.format(Source.AugmentedDiffs).options(options).load

              // aggregations are triggered when an event with a later timestamp ("event time") is received
              val query = geoms.writeStream
                .format("console")
                .start

              query.awaitTermination()

              ss.stop()
          }
      }
    )
