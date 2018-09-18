package osmesa.analytics.oneoffs

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql._
import osmesa.analytics.Analytics
import osmesa.common.ProcessOSM

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * # Running an infinite stream from the beginning of time
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.ChangeStreamProcessor \
 *   ./analytics/target/scala-2.11/osmesa-analytics.jar \
 *   --start-sequence 1
 *
 * This class prints the change stream out to console for debugging
 */
object ChangeStreamProcessor extends CommandApp(
  name = "osmesa-diff-stream-processor",
  header = "display diffs from a change stream",
  main = {
    val changeSourceOpt =
      Opts.option[URI](
        "change-source",
        short = "c",
        metavar = "uri",
        help = "Location of changes to process"
      ).withDefault(new URI("https://planet.osm.org/replication/minute/"))
    val startSequenceOpt =
      Opts.option[Int](
        "start-sequence",
        short = "s",
        metavar = "sequence",
        help = "Starting sequence. If absent, the current (remote) sequence will be used."
      ).orNone
    val endSequenceOpt =
      Opts.option[Int](
        "end-sequence",
        short = "e",
        metavar = "sequence",
        help = "Ending sequence. If absent, this will be an infinite stream."
      ).orNone
    val databaseUriOpt =
      Opts.option[URI](
        "database-uri",
        short = "d",
        metavar = "database URL",
        help = "Database URL (default: $DATABASE_URL environment variable)"
      ).withDefault(sys.env.get("DATABASE_URL"))

    (changeSourceOpt, startSequenceOpt, endSequenceOpt, databaseUriOpt).mapN {
      (changeSource, startSequence, endSequence, databaseUri) =>
        implicit val ss: SparkSession =
          Analytics.sparkSession("ChangeStreamProcessor")

        import ss.implicits._

        val options = Map(
          "base_uri"  -> changeSource.toString,
          "proc_name" -> "ChangeStream"
        ) ++
          databaseUri
            .map(db => Map("db_uri" -> db.toString))
            .getOrElse(Map.empty[String, String]) ++
          startSequence
            .map(s => Map("start_sequence" -> s.toString))
            .getOrElse(Map.empty[String, String]) ++
          endSequence
            .map(s => Map("end_sequence" -> s.toString))
            .getOrElse(Map.empty[String, String])

        val changes =
          ss.readStream
            .format("changes")
            .options(options)
            .load

        val changeProcessor = changes
          .select('id, 'version, 'lat, 'lon, 'visible)
          .where('_type === ProcessOSM.NodeType and !'visible)
          .writeStream
          .queryName("display change data")
          .format("console")
          .start

        changeProcessor.awaitTermination()

        ss.stop()
    }
  }
)
