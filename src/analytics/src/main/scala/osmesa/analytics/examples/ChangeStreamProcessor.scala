package osmesa.analytics.examples

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql._
import osmesa.analytics.Analytics
import osmesa.common.sources.Source

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.examples.ChangeStreamProcessor \
 *   ingest/target/scala-2.11/osmesa-analytics.jar \
 *   --augmented-diff-source s3://somewhere/diffs/
 */
object ChangeStreamProcessor
    extends CommandApp(
      name = "osmesa-change-stream-processor",
      header = "Update statistics from streaming changes",
      main = {
        val changeSourceOpt = Opts
          .option[URI]("change-source",
                       short = "d",
                       metavar = "uri",
                       help = "Location of minutely diffs to process")
          .withDefault(new URI("https://planet.osm.org/replication/minute/"))
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
        val batchSizeOpt = Opts
          .option[Int]("batch-size",
                       short = "b",
                       metavar = "batch size",
                       help = "Change batch size.")
          .orNone

        (changeSourceOpt, startSequenceOpt, endSequenceOpt, batchSizeOpt)
          .mapN {
            (changeSource, startSequence, endSequence, batchSize) =>
              implicit val ss: SparkSession =
                Analytics.sparkSession("ChangeStreamProcessor")

              val options = Map(Source.BaseURI -> changeSource.toString, Source.ProcessName -> "ChangeStreamProcessor") ++
                startSequence.map(s => Map(Source.StartSequence -> s.toString))
                  .getOrElse(Map.empty[String, String]) ++
                endSequence.map(s => Map(Source.EndSequence -> s.toString))
                  .getOrElse(Map.empty[String, String]) ++
                batchSize.map(s => Map(Source.BatchSize -> s.toString))
                  .getOrElse(Map.empty[String, String])

              val changes =
                ss.readStream.format(Source.Changes).options(options).load

              val query = changes.writeStream
                .format("console")
                .start

              query.awaitTermination()

              ss.stop()
          }
      }
    )
