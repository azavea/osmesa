package osmesa.analytics.oneoffs

import java.io._
import java.net.URI

import cats.implicits._
import com.monovore.decline._
import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import osmesa.analytics.{Analytics, EditHistogram}
import osmesa.common.model._
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
        type AugmentedDiffFeature = Feature[Geometry, ElementWithSequence]
        val rootURI = new File("").toURI

        val changeSourceOpt = Opts
          .option[URI]("source",
                       short = "d",
                       metavar = "uri",
                       help = "Location of minutely diffs to process")
          .withDefault(new URI("https://planet.osm.org/replication/minute/"))
        // TODO this is off-by-one when running from the DB
        val changesStartSequenceOpt = Opts
          .option[Int](
            "start-sequence",
            short = "s",
            metavar = "sequence",
            help =
              "Minutely diff starting sequence. If absent, the current (remote) sequence will be used.")
          .orNone
        val changesEndSequenceOpt = Opts
          .option[Int](
            "end-sequence",
            short = "e",
            metavar = "sequence",
            help = "Minutely diff ending sequence. If absent, this will be an infinite stream.")
          .orNone
        val changesBatchSizeOpt = Opts
          .option[Int]("batch-size",
                       short = "b",
                       metavar = "batch size",
                       help = "Change batch size.")
          .orNone
        val tileSourceOpt = Opts
          .option[URI](
            "tile-source",
            short = "t",
            metavar = "uri",
            help = "URI prefix of MVTs to update"
          )
          .withDefault(rootURI)
        val concurrentUploadsOpt = Opts
          .option[Int]("concurrent-uploads",
                       short = "c",
                       metavar = "concurrent uploads",
                       help = "Set the number of concurrent uploads.")
          .orNone
        val databaseUriOpt =
          Opts.option[URI](
            "database-uri",
            short = "d",
            metavar = "database URL",
            help = "Database URL (default: $DATABASE_URL environment variable)"
          )
        val databaseUriEnv =
          Opts.env[URI]("DATABASE_URL", help = "The URL of the database")

        (changeSourceOpt,
         changesStartSequenceOpt,
         changesEndSequenceOpt,
         changesBatchSizeOpt,
         tileSourceOpt,
         concurrentUploadsOpt,
         databaseUriOpt orElse databaseUriEnv).mapN {
          (changeSource,
           changesStartSequence,
           changesEndSequence,
           changesBatchSize,
           tileSource,
           _concurrentUploads,
           databaseUri) =>
            val AppName = "EditHistogramTileUpdater"

            val spark: SparkSession = Analytics.sparkSession(AppName)
            import spark.implicits._
            implicit val concurrentUploads: Option[Int] = _concurrentUploads

            val changeOptions = Map(Source.BaseURI -> changeSource.toString,
                Source.DatabaseURI -> databaseUri.toString,
                Source.ProcessName -> AppName) ++
              changesStartSequence.map(s => Map(Source.StartSequence -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              changesEndSequence.map(s => Map(Source.EndSequence -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              changesBatchSize.map(s => Map(Source.BatchSize -> s.toString))
                .getOrElse(Map.empty[String, String])

            val changes = spark.readStream.format(Source.Changes)
              .options(changeOptions)
              .load

            val changedNodes = changes
              .where('type === "node" and 'lat.isNotNull and 'lon.isNotNull)
              .select('sequence, year('timestamp) * 1000 + dayofyear('timestamp) as 'key, 'lat, 'lon)

            val tiledNodes = EditHistogram.update(changedNodes, tileSource)

            val query = tiledNodes.writeStream
              .queryName("edit histogram tiles")
              .format("console")
              .start

            query.awaitTermination()

            spark.stop()
        }
      }
    )
