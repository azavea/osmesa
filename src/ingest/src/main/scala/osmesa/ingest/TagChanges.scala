package osmesa.ingest

import cats.implicits._
import com.monovore.decline._
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

/*
 * Usage example:
 *
 * sbt "project ingest" assembly
 *
 * spark-submit \
 *   --class osmesa.ingest.TagChanges \
 *   ingest/target/scala-2.11/osmesa-ingest.jar \
 *   --orc=$HOME/data/osm/isle-of-man.orc \
 *   --out=$HOME/data/osm/isle-of-man-geoms.orc \
 */

object TagChanges extends CommandApp(
      name = "osmesa-tag-changes",
      header = "Generate tag differences between element versions",
      main = {

        /* CLI option handling */
        val orcOpt = Opts
          .option[String]("orc", help = "Location of the ORC file to process")
        val outOpt =
          Opts.option[String]("out", help = "ORC file containing geometries")
        val numPartitionsOpt = Opts
          .option[Int]("partitions", help = "Number of partitions to generate")
          .withDefault(1)

        (orcOpt, outOpt, numPartitionsOpt).mapN {
          (orc, out, numPartitions) =>
            /* Settings compatible for both local and EMR execution */
            val conf = new SparkConf()
              .setIfMissing("spark.master", "local[*]")
              .setAppName("make-geometries")
              .set(
                "spark.serializer",
                classOf[org.apache.spark.serializer.KryoSerializer].getName
              )
              .set(
                "spark.kryo.registrator",
                classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName
              )
              .set("spark.ui.showConsoleProgress", "true")

            implicit val ss: SparkSession = SparkSession.builder
              .config(conf)
              .enableHiveSupport
              .getOrCreate

            import ss.implicits._

            /* Silence the damn INFO logger */
            Logger.getRootLogger.setLevel(Level.WARN)

            val df = ss.read.orc(orc)

            @transient val idByVersion =
              Window.partitionBy('id).orderBy('version)

            val _tagsAdded =
              (prev: Map[String, String], curr: Map[String, String]) => {
                if (prev == null) {
                  curr
                } else {
                  (curr.toSet diff prev.toSet).toMap
                }
              }

            val tagsAdded: UserDefinedFunction = udf(_tagsAdded)

            val _tagsRemoved =
              (prev: Map[String, String], curr: Map[String, String]) => {
                if (prev == null) {
                  Map.empty[String, String]
                } else {
                  (prev.toSet diff curr.toSet).toMap
                }
              }

            val tagsRemoved: UserDefinedFunction = udf(_tagsRemoved)

            df.withColumn("prevTags", lag('tags, 1) over idByVersion)
              .withColumn("tagsAdded", tagsAdded('prevTags, 'tags))
              .withColumn("tagsRemoved", tagsRemoved('prevTags, 'tags))
              .drop('lat)
              .drop('lon)
              .drop('nds)
              .drop('members)
              .repartition(numPartitions)
              .write
              .mode(SaveMode.Overwrite)
              .orc(out)

            ss.stop()

            println("Done.")
        }
      }
    )
