package osmesa.analytics.examples

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql._
import osmesa.analytics.Analytics
import vectorpipe.model.Changeset
import vectorpipe.sources.Source

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.examples.ChangesetProcessor \
 *   ingest/target/scala-2.11/osmesa-analytics.jar
 */
object ChangesetProcessor
  extends CommandApp(
    name = "osmesa-changeset-processor",
    header = "Read from changesets",
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

      (changesetSourceOpt, startSequenceOpt, endSequenceOpt)
        .mapN {
          (changesetSource, startSequence, endSequence) =>
            implicit val ss: SparkSession =
              Analytics.sparkSession("ChangesetProcessor")

            import ss.implicits._

            val options = Map(Source.BaseURI -> changesetSource.toString) ++
              startSequence
                .map(s => Map(Source.StartSequence -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              endSequence
                .map(s => Map(Source.EndSequence -> s.toString))
                .getOrElse(Map.empty[String, String])

            val changes =
              ss.read.format(Source.Changesets).options(options).load

            // aggregations are triggered when an event with a later timestamp ("event time") is received
            // changes.select('sequence).distinct.show
            changes.as[Changeset].show

            ss.stop()
        }
    }
  )
