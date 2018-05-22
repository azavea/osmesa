package osmesa.analytics.oneoffs

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import geotrellis.vector.{Feature, Geometry}
import org.apache.spark._
import org.apache.spark.sql._
import osmesa.common.AugmentedDiff

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.ChangesetStreamProcessor \
 *   ingest/target/scala-2.11/osmesa-analytics.jar \
 *   --changeset-source s3://somewhere/diffs/ \
 *   --database-url $DATABASE_URL
 */
object ChangesetStreamProcessor
    extends CommandApp(
      name = "osmesa-augmented-diff-stream-processor",
      header = "Update statistics from streaming augmented diffs",
      main = {
        type AugmentedDiffFeature = Feature[Geometry, AugmentedDiff]

        val changesetSourceOpt =
          Opts
            .option[URI]("changeset-source",
                         short = "c",
                         metavar = "uri",
                         help = "Location of changesets to process")
            .withDefault(new URI("https://planet.osm.org/replication/changesets/"))
        val databaseUrlOpt = Opts
          .option[URI]("database-url", short = "d", metavar = "database URL", help = "Database URL")
          .orNone

        (changesetSourceOpt, databaseUrlOpt).mapN {
          (changesetSource, databaseUri) =>
            /* Settings compatible for both local and EMR execution */
            val conf = new SparkConf()
              .setIfMissing("spark.master", "local[*]")
              .setAppName("changeset-stream-processor")
              .set("spark.serializer", classOf[org.apache.spark.serializer.KryoSerializer].getName)
              .set("spark.kryo.registrator",
                   classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)

            implicit val ss: SparkSession = SparkSession.builder
              .config(conf)
              .enableHiveSupport
              .getOrCreate

            import ss.implicits._

            val changesets =
              ss.readStream
                .format("osmesa.analytics.streaming.ChangesetsProvider")
                .option("base_uri", "https://planet.osm.org/replication/changesets/")
//                .option("start_sequence", 2901438)
//                .option("end_sequence", 2901440)
                .load

            val changesetProcessor = changesets
//              .withWatermark("created_at", "5 seconds")
              .select('id, 'tags, 'created_at)
            .writeStream
              .format("console")
//              .foreach(new ForeachWriter[Row] {
//                override def open(partitionId: Long, version: Long): Boolean = true
//
//                override def process(value: Row): Unit = {
//                  val sequence = value.getAs[Int]("sequence")
//                  val id = value.getAs[Long]("id")
//                  val tags = value.getAs[Map[String, String]]("tags")
//
//                  println(sequence, id, tags.getOrElse("comment", ""))
//                }
//
//                override def close(errorOrNull: Throwable): Unit = Unit
//              })
              .start

            changesetProcessor.awaitTermination()

            ss.stop()
        }
      }
    )
