package osmesa.analytics.oneoffs

import java.io._
import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql._
import osmesa.analytics.{Analytics, Footprints}
import osmesa.common.sources.Source

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.UserFootprintUpdater \
 *   ingest/target/scala-2.11/osmesa-analytics.jar
 */
object UserFootprintUpdater
    extends CommandApp(
      name = "osmesa-user-footprint-updater",
      header = "Consume minutely diffs to update user footprint MVTs",
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
            help =
              "Minutely diff starting sequence. If absent, the current (remote) sequence will be used.")
          .orNone

        val endSequenceOpt = Opts
          .option[Int](
            "end-sequence",
            short = "e",
            metavar = "sequence",
            help =
              "Minutely diff ending sequence. If absent, the current (remote) sequence will be used.")
          .orNone

        val partitionCountOpt = Opts
          .option[Int]("partition-count",
                       short = "p",
                       metavar = "partition count",
                       help = "Change partition count.")
          .orNone

        val tileSourceOpt = Opts
          .option[URI](
            "tile-source",
            short = "t",
            metavar = "uri",
            help = "URI prefix for vector tiles to update"
          )
          .withDefault(new File("").toURI)

        val concurrentUploadsOpt = Opts
          .option[Int]("concurrent-uploads",
                       short = "c",
                       metavar = "concurrent uploads",
                       help = "Set the number of concurrent uploads.")
          .orNone

        (changeSourceOpt,
         startSequenceOpt,
         endSequenceOpt,
         partitionCountOpt,
         tileSourceOpt,
         concurrentUploadsOpt).mapN {
          (changeSource,
           startSequence,
           endSequence,
           partitionCount,
           tileSource,
           _concurrentUploads) =>
            val AppName = "UserFootprintUpdater"

            val spark: SparkSession = Analytics.sparkSession(AppName)
            import spark.implicits._
            implicit val concurrentUploads: Option[Int] = _concurrentUploads

            val changeOptions = Map(Source.BaseURI -> changeSource.toString) ++
              startSequence
                .map(x => Map(Source.StartSequence -> x.toString))
                .getOrElse(Map.empty[String, String]) ++
              endSequence
                .map(x => Map(Source.EndSequence -> x.toString))
                .getOrElse(Map.empty[String, String]) ++
              partitionCount
                .map(x => Map(Source.PartitionCount -> x.toString))
                .getOrElse(Map.empty[String, String])

            val changes = spark.read
              .format(Source.Changes)
              .options(changeOptions)
              .load

            val changedNodes = changes
              .where('type === "node" and 'lat.isNotNull and 'lon.isNotNull)
              .select('sequence, 'uid as 'key, 'lat, 'lon)

            val tiledNodes =
              Footprints.update(changedNodes, tileSource)

            print(s"${tiledNodes.count} tiles updated.")
        }
      }
    )
