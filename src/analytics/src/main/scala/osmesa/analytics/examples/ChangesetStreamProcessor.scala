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
 *   --class osmesa.analytics.examples.ChangesetStreamProcessor \
 *   ingest/target/scala-2.11/osmesa-analytics.jar \
 *   --augmented-diff-source s3://somewhere/diffs/
 */
object ChangesetStreamProcessor
  extends CommandApp(
    name = "osmesa-changeset-stream-processor",
    header = "Update statistics from streaming changesets",
    main = {
      val changesetSourceOpt =
        Opts.option[URI]("changeset-source",
          short = "c",
          metavar = "uri",
          help = "Location of changesets to process"
        ).withDefault(new URI("https://planet.osm.org/replication/changesets/"))
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

      (changesetSourceOpt, startSequenceOpt, endSequenceOpt, batchSizeOpt)
        .mapN {
          (changesetSource, startSequence, endSequence, batchSize) =>
            implicit val ss: SparkSession =
              Analytics.sparkSession("ChangesetStreamProcessor")

            val options = Map(Source.BaseURI -> changesetSource.toString, Source.ProcessName -> "ChangesetStreamProcessor") ++
              startSequence.map(s => Map(Source.StartSequence -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              endSequence.map(s => Map(Source.EndSequence -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              batchSize.map(s => Map(Source.BatchSize -> s.toString))
                .getOrElse(Map.empty[String, String])

            val changesets =
              ss.readStream.format(Source.Changesets).options(options).load

            val query = changesets.writeStream
              .format("console")
              .start

            query.awaitTermination()

            ss.stop()
        }
    }
  )
