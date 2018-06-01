package osmesa.analytics.oneoffs

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark._
import org.apache.spark.sql._

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.ChangeStreamProcessor \
 *   ingest/target/scala-2.11/osmesa-analytics.jar \
 *   --database-url $DATABASE_URL
 */
object ChangeStreamProcessor
    extends CommandApp(
      name = "osmesa-diff-stream-processor",
      header = "display diffs from a change stream",
      main = {
        val changesetSourceOpt =
          Opts
            .option[URI]("change-source",
                         short = "c",
                         metavar = "uri",
                         help = "Location of changesets to process")
            .withDefault(new URI("https://planet.osm.org/replication/minute/"))
        val startSequenceOpt = Opts
          .option[Int](
            "start-sequence",
            short = "s",
            metavar = "sequence",
            help = "Starting sequence. If absent, the current (remote) sequence will be used.")
          .orNone
        val endSequenceOpt = Opts
          .option[Int]("end-sequence",
                       short = "e",
                       metavar = "sequence",
                       help = "Ending sequence. If absent, this will be an infinite stream.")
          .orNone

        (changesetSourceOpt, startSequenceOpt, endSequenceOpt).mapN {
          (changesetSource, startSequence, endSequence) =>
            /* Settings compatible for both local and EMR execution */
            val conf = new SparkConf()
              .setIfMissing("spark.master", "local[*]")
              .setAppName("change-stream-processor")
              .set("spark.serializer", classOf[org.apache.spark.serializer.KryoSerializer].getName)
              .set("spark.kryo.registrator",
                   classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)

            implicit val ss: SparkSession = SparkSession.builder
              .config(conf)
              .enableHiveSupport
              .getOrCreate

            val options = Map("base_uri" -> changesetSource.toString) ++
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

            val changeProcessor = changes.writeStream
              .queryName("display change data")
              .format("console")
              .start

            changeProcessor.awaitTermination()

            ss.stop()
        }
      }
    )
