package osmesa.analytics.oneoffs

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import geotrellis.vector.{Feature, Geometry}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import vectorpipe.functions.osm._
import vectorpipe.model.ElementWithSequence
import vectorpipe.sources.Source

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.MergedChangesetStreamProcessor \
 *   ingest/target/scala-2.11/osmesa-analytics.jar \
 *   --augmented-diff-source s3://somewhere/diffs/ \
 */
object MergedChangesetStreamProcessor extends CommandApp(
  name = "osmesa-merged-changeset-stream-processor",
  header = "Consume augmented diffs + changesets and join them",
  main = {
    type AugmentedDiffFeature = Feature[Geometry, ElementWithSequence]

    val augmentedDiffSourceOpt =
      Opts.option[URI](
        "augmented-diff-source",
        short = "a",
        metavar = "uri",
        help = "Location of augmented diffs to process"
      )
    val changesetSourceOpt =
      Opts.option[URI](
        "changeset-source",
        short = "c",
        metavar = "uri",
        help = "Location of changesets to process"
      ).withDefault(new URI("https://planet.osm.org/replication/changesets/"))
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
    val diffStartSequenceOpt =
      Opts.option[Int](
        "diff-start-sequence",
        short = "S",
        metavar = "sequence",
        help = "Starting augmented diff sequence. If absent, the current (remote) sequence will be used."
      ).orNone
    val diffEndSequenceOpt =
      Opts.option[Int](
        "diff-end-sequence",
        short = "E",
        metavar = "sequence",
        help = "Ending augmented diff sequence. If absent, this will be an infinite stream."
      ).orNone

    (augmentedDiffSourceOpt,
     changesetSourceOpt,
     startSequenceOpt,
     endSequenceOpt,
     diffStartSequenceOpt,
     diffEndSequenceOpt).mapN {
      (augmentedDiffSource,
       changesetSource,
       startSequence,
       endSequence,
       diffStartSequence,
       diffEndSequence) =>
        /* Settings compatible for both local and EMR execution */
        val conf = new SparkConf()
          .setIfMissing("spark.master", "local[*]")
          .setAppName("merged-changeset-stream-processor")
          .set(
            "spark.serializer",
            classOf[org.apache.spark.serializer.KryoSerializer].getName
          )
          .set(
            "spark.kryo.registrator",
            classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName
          )

        implicit val ss: SparkSession = SparkSession.builder
          .config(conf)
          .enableHiveSupport
          .getOrCreate

        import ss.implicits._

        val augmentedDiffOptions = Map(
          Source.BaseURI -> augmentedDiffSource.toString,
          Source.ProcessName -> "MergedChangesetStream"
        ) ++
          diffStartSequence
            .map(s => Map(Source.StartSequence -> s.toString))
            .getOrElse(Map.empty[String, String]) ++
          diffEndSequence
            .map(s => Map(Source.EndSequence -> s.toString))
            .getOrElse(Map.empty[String, String])

        val geoms = ss.readStream
          .format(Source.AugmentedDiffs)
          .options(augmentedDiffOptions)
          .load

        val changesetOptions = Map(Source.BaseURI -> changesetSource.toString, Source.ProcessName -> "MergedChangesetStream") ++
          startSequence
            .map(s => Map(Source.StartSequence -> s.toString))
            .getOrElse(Map.empty[String, String]) ++
          endSequence
            .map(s => Map(Source .EndSequence -> s.toString))
            .getOrElse(Map.empty[String, String])

        val changesets =
          ss.readStream
            .format(Source.Changesets)
            .options(changesetOptions)
            .load

        val changesetsWithWatermark = changesets
        // changesets can remain open for 24 hours; buy some extra time
        // TODO can projecting into the future (createdAt + 24 hours) and coalescing closedAt reduce the number
        // of changesets being tracked?
          .withWatermark("createdAt", "25 hours")
          .select(
            'id as 'changeset,
            'tags.getField("created_by") as 'editor,
            hashtags('tags.getField("comment")) as 'hashtags
          )

        val geomsWithWatermark = geoms
          .withColumn(
            "timestamp",
            to_timestamp('sequence * 60 + 1347432900)
          )
          // geoms are standalone; no need to wait for anything
          .withWatermark("timestamp", "0 seconds")
          .select('timestamp, 'changeset, '_type, 'id, 'version,
            'minorVersion, 'updated)

        val query = geomsWithWatermark
          .join(changesetsWithWatermark, Seq("changeset"))
          .writeStream
          .queryName("merge features w/ changeset metadata")
          .format("console")
          .start

        query.awaitTermination()

        ss.stop()
    }
  }
)
