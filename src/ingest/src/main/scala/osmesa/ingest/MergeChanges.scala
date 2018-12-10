package osmesa

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark._
import org.apache.spark.sql._
import osmesa.common.sources.Source

/*
 * Usage example:
 *
 * sbt "project ingest" assembly
 *
 * spark-submit \
 *   --class osmesa.MergeChanges \
 *   ingest/target/scala-2.11/osmesa-ingest.jar \
 *   --out=$HOME/data/osm/isle-of-man.osh.orc \
 */

object MergeChanges
    extends CommandApp(
      name = "osmesa-make-geometries",
      header = "Create geometries from an ORC file",
      main = {

        /* CLI option handling */
        val outO = Opts.option[String]("out", help = "ORC file containing merged changes")
        val numPartitionsO =
          Opts.option[Int]("partitions", help = "Number of partitions to generate").withDefault(1)
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

        (changeSourceOpt, startSequenceOpt, endSequenceOpt, outO, numPartitionsO).mapN {
          (changeSource, startSequence, endSequence, out, numPartitions) =>
            /* Settings compatible for both local and EMR execution */
            val conf = new SparkConf()
              .setIfMissing("spark.master", "local[*]")
              .setAppName("update-history")
              .set("spark.ui.showConsoleProgress", "true")
              .set("spark.serializer", classOf[org.apache.spark.serializer.KryoSerializer].getName)
              .set("spark.kryo.registrator",
                   classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)

            implicit val spark: SparkSession = SparkSession.builder
              .config(conf)
              .enableHiveSupport
              .getOrCreate

            val options = Map(Source.BaseURI -> changeSource.toString, Source.BatchSize -> "1") ++
              startSequence
                .map(s => Map(Source.StartSequence -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              endSequence
                .map(s => Map(Source.EndSequence -> s.toString))
                .getOrElse(Map.empty[String, String])

            val changes =
              spark.read.format(Source.Changes).options(options).load

            changes.repartition(numPartitions).write.mode(SaveMode.Overwrite).orc(out)
        }
      }
    )
