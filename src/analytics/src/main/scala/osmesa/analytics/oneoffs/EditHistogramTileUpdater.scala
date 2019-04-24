package osmesa.analytics.oneoffs

import java.io._
import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import osmesa.analytics.{Analytics, EditHistogram}
import osmesa.common.sources.Source

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.EditHistogramTileUpdater \
 *   ingest/target/scala-2.11/osmesa-analytics.jar
 */
object EditHistogramTileUpdater
    extends CommandApp(
      name = "osmesa-edit-histogram-tile-updater",
      header = "Consume minutely diffs to update edit histogram MVTs",
      main = {
        val changeSourceOpt = Opts
          .option[URI]("source",
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
            help = "URI prefix of MVTs to update"
          )
          .withDefault(new File("").toURI)

        val concurrentUploadsOpt = Opts
          .option[Int]("concurrent-uploads",
                       short = "c",
                       metavar = "concurrent uploads",
                       help = "Set the number of concurrent uploads.")
          .orNone

        val baseZoomOpt = Opts
          .option[Int]("base-zoom",
                       short = "z",
                       metavar = "Base zoom",
                       help = "Most detailed zoom level")
          .orNone

        (changeSourceOpt,
         startSequenceOpt,
         endSequenceOpt,
         partitionCountOpt,
         tileSourceOpt,
         concurrentUploadsOpt,
         baseZoomOpt).mapN {
          (changeSource,
           startSequence,
           endSequence,
           partitionCount,
           tileSource,
           _concurrentUploads,
           baseZoom) =>
            val AppName = "EditHistogramTileUpdater"

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
              .select('sequence,
                      year('timestamp) * 1000 + dayofyear('timestamp) as 'key,
                      'lat,
                      'lon)

            val tiledNodes = EditHistogram.update(changedNodes,
                                                  tileSource,
                                                  baseZoom.getOrElse(EditHistogram.DefaultBaseZoom))

            val lastSequence =
              changedNodes.select(max('sequence) as 'sequence).first.getAs[Int]("sequence")

            println(s"${tiledNodes.count} tiles updated to ${lastSequence}.")
        }
      }
    )
