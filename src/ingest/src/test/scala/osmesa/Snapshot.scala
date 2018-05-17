package osmesa

import java.sql.Timestamp

import cats.implicits._
import com.monovore.decline._
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import osmesa.common.ProcessOSM


/*
 * Usage example:
 *
 * sbt "project ingest" assembly
 *
 * spark-submit \
 *   --class osmesa.Snapshot \
 *   ingest/target/scala-2.11/osmesa-ingest.jar \
 *   --orc $HOME/data/rhode-island.orc \
 *   --out $HOME/data/rhode-island-2009 \
 *   --timestamp "2009-01-01 00:00:00"
 */

object Snapshot extends CommandApp(
  name = "snapshot",
  header = "Create a snapshot",
  main = {

    /* CLI option handling */
    val orcO = Opts.option[String]("orc", help = "Location of the ORC file to process")
    val outO = Opts.option[String]("out", help = "Location of the ORC file to write")
    val timestampO = Opts.option[String]("timestamp", help = "Timestamp to snapshot from").withDefault(null)

    (orcO, outO, timestampO).mapN { (orc, out, timestampS) =>
      val timestamp = Option(timestampS).map(Timestamp.valueOf).orNull

      /* Settings compatible with both local and EMR execution */
      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("extract-multipolygons")
        .set("spark.serializer", classOf[org.apache.spark.serializer.KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)
        .set("spark.sql.orc.impl", "native")

      implicit val ss: SparkSession = SparkSession.builder
        .config(conf)
        .enableHiveSupport
        .getOrCreate

      import ss.implicits._

      // quiet Spark
      Logger.getRootLogger.setLevel(Level.WARN)

      val df = ss.read.orc(orc)

      val nodes = ProcessOSM.preprocessNodes(df)
        .select(
          'id,
          lit("node"),
          'tags,
          'lat,
          'lon,
          typedLit[List[Any]](null).as('nds),
          typedLit[List[Any]](null).as('members),
          'changeset,
          'timestamp,
          'validUntil,
          'uid,
          'user,
          'version,
          'visible)

      val ways = ProcessOSM.preprocessWays(df)
        .select(
          'id,
          lit("way"),
          'tags,
          lit(Double.NaN).as('lat),
          lit(Double.NaN).as('lon),
          'nds,
          typedLit[List[Any]](null).as('members),
          'changeset,
          'timestamp,
          'validUntil,
          'uid,
          'user,
          'version,
          'visible)

      val relations = ProcessOSM.preprocessRelations(df)
        .select(
          'id,
          lit("relation"),
          'tags,
          lit(Double.NaN).as('lat),
          lit(Double.NaN).as('lon),
          typedLit[List[Any]](null).as('nds),
          'members,
          'changeset,
          'timestamp,
          'validUntil,
          'uid,
          'user,
          'version,
          'visible)

      val elements = nodes
        .union(ways)
        .union(relations)

      ProcessOSM.snapshot(elements, timestamp)
        .repartition(1)
        .write
        .orc(out)

      ss.stop()

      println("Done.")
    }
  }
)