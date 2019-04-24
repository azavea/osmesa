package osmesa.analytics.oneoffs

import java.io._
import java.net.URI

import cats.implicits._
import com.monovore.decline._
import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import osmesa.analytics.{Analytics, Footprints}
import osmesa.common.ProcessOSM
import osmesa.common.functions.osm._
import osmesa.common.model.ElementWithSequence
import osmesa.common.sources.Source

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.HashtagFootprintUpdater \
 *   ingest/target/scala-2.11/osmesa-analytics.jar
 */
object HashtagFootprintUpdater
    extends CommandApp(
      name = "osmesa-hashtag-footprint-updater",
      header = "Consume minutely diffs + changesets and update hashtag footprint MVTs",
      main = {
        type AugmentedDiffFeature = Feature[Geometry, ElementWithSequence]
        val rootURI = new File("").toURI

        val changeSourceOpt =
          Opts
            .option[URI]("change-source",
                         short = "d",
                         metavar = "uri",
                         help = "Location of minutely diffs to process")
            .withDefault(new URI("https://planet.osm.org/replication/minute/"))
        val changesStartSequenceOpt =
          Opts
            .option[Int](
              "changes-start-sequence",
              short = "s",
              metavar = "sequence",
              help =
                "Minutely diff starting sequence. If absent, the current (remote) sequence will be used."
            )
            .orNone
        val changesEndSequenceOpt =
          Opts
            .option[Int](
              "changes-end-sequence",
              short = "e",
              metavar = "sequence",
              help = "Minutely diff ending sequence. If absent, this will be an infinite stream."
            )
            .orNone
        val changesBatchSizeOpt =
          Opts
            .option[Int]("changes-batch-size",
                         short = "b",
                         metavar = "batch size",
                         help = "Change batch size.")
            .orNone
        val changesetSourceOpt =
          Opts
            .option[URI]("changeset-source",
                         short = "c",
                         metavar = "uri",
                         help = "Location of changesets to process")
            .withDefault(new URI("https://planet.osm.org/replication/changesets/"))
        val changesetsStartSequenceOpt =
          Opts
            .option[Int](
              "changesets-start-sequence",
              short = "S",
              metavar = "sequence",
              help =
                "Changeset starting sequence. If absent, the current (remote) sequence will be used."
            )
            .orNone
        val changesetsEndSequenceOpt =
          Opts
            .option[Int]("changesets-end-sequence",
                         short = "E",
                         metavar = "sequence",
                         help =
                           "Changeset ending sequence. If absent, this will be an infinite stream.")
            .orNone
        val changesetsBatchSizeOpt =
          Opts
            .option[Int]("changesets-batch-size",
                         short = "B",
                         metavar = "batch size",
                         help = "Changeset batch size.")
            .orNone
        val tileSourceOpt =
          Opts
            .option[URI](
              "tile-source",
              short = "t",
              metavar = "uri",
              help = "URI prefix for vector tiles to update"
            )
            .withDefault(rootURI)
        val concurrentUploadsOpt = Opts
          .option[Int]("concurrent-uploads",
                       short = "c",
                       metavar = "concurrent uploads",
                       help = "Set the number of concurrent uploads.")
          .orNone

        (changeSourceOpt,
         changesStartSequenceOpt,
         changesEndSequenceOpt,
         changesBatchSizeOpt,
         changesetSourceOpt,
         changesetsStartSequenceOpt,
         changesetsEndSequenceOpt,
         changesetsBatchSizeOpt,
         tileSourceOpt,
         concurrentUploadsOpt).mapN {
          (changeSource,
           changesStartSequence,
           changesEndSequence,
           changesBatchSize,
           changesetSource,
           changesetsStartSequence,
           changesetsEndSequence,
           changesetsBatchSize,
           tileSource,
           _concurrentUploads) =>
            implicit val spark: SparkSession = Analytics.sparkSession("HashtagFootprintUpdater")
            import spark.implicits._
            implicit val concurrentUploads: Option[Int] = _concurrentUploads

            val changeOptions = Map(Source.BaseURI -> changeSource.toString,
                                    Source.ProcessName -> "HashtagFootprintUpdater") ++
              changesStartSequence
                .map(s => Map(Source.StartSequence -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              changesEndSequence
                .map(s => Map(Source.EndSequence -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              changesBatchSize
                .map(s => Map(Source.BatchSize -> s.toString))
                .getOrElse(Map.empty[String, String])

            val changes = spark.readStream
              .format(Source.Changes)
              .options(changeOptions)
              .load

            val changesetOptions = Map(Source.BaseURI -> changesetSource.toString,
                                       Source.ProcessName -> "HashtagFootprintUpdater") ++
              changesetsStartSequence
                .map(s => Map(Source.StartSequence -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              changesetsEndSequence
                .map(s => Map(Source.EndSequence -> s.toString))
                .getOrElse(Map.empty[String, String])
            changesetsBatchSize
              .map(s => Map(Source.BatchSize -> s.toString))
              .getOrElse(Map.empty[String, String])

            val changesets = spark.readStream
              .format(Source.Changesets)
              .options(changesetOptions)
              .load
              // changesets can remain open for 24 hours; buy some extra time
              // TODO can projecting into the future (createdAt + 24 hours) and coalescing closedAt reduce the number
              // of changesets being tracked?
              .withWatermark("createdAt", "25 hours")
              .withColumn("hashtag", explode(hashtags('tags.getField("comment"))))
              .select('sequence, 'id as 'changeset, 'hashtag as 'key)

            val changedNodes = changes
            // geoms may appear before the changeset they're within or the changeset metadata may have been missed, in
            // which case watch for it to be closed within the next 24 hours
              .withWatermark("timestamp", "25 hours")
              .where('_type === ProcessOSM.NodeType and 'lat.isNotNull and 'lon.isNotNull)
              .select('timestamp, 'changeset, 'lat, 'lon)

            val changedNodesWithHashtags = changesets
              .join(changedNodes, Seq("changeset"))

            val tiledNodes =
              Footprints.update(changedNodesWithHashtags, tileSource)

            val query = tiledNodes.writeStream
              .queryName("tiled hashtag footprints")
              .format("console")
              .start

            query.awaitTermination()

            spark.stop()
        }
      }
    )
