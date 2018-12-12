package osmesa.analytics.examples

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql._
import osmesa.analytics.Analytics
import osmesa.common.model.Change
import osmesa.common.sources.Source

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.examples.ChangeProcessor \
 *   ingest/target/scala-2.11/osmesa-analytics.jar
 */
object ChangeProcessor
  extends CommandApp(
    name = "osmesa-change-processor",
    header = "Read from changes",
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

      (changeSourceOpt, startSequenceOpt, endSequenceOpt)
        .mapN {
          (changeSource, startSequence, endSequence) =>
            implicit val ss: SparkSession =
              Analytics.sparkSession("ChangeProcessor")

            import ss.implicits._

            val options = Map(Source.BaseURI -> changeSource.toString) ++
              startSequence
                .map(s => Map(Source.StartSequence -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              endSequence
                .map(s => Map(Source.EndSequence -> s.toString))
                .getOrElse(Map.empty[String, String])

            val changes =
              ss.read.format(Source.Changes).options(options).load

            // aggregations are triggered when an event with a later timestamp ("event time") is received
            // changes.select('sequence).distinct.show
            changes.as[Change].show

            ss.stop()
        }
    }
  )
