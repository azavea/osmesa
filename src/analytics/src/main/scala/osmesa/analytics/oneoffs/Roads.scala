package osmesa.analytics.oneoffs

import scala.util.{Try, Success, Failure}

import cats.implicits._
import com.monovore.decline._
import geotrellis.vector.{Feature, Line}
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import vectorpipe._

// --- //

/* To run:
 run --orc "/home/colin/code/azavea/vectorpipe/data/isle-of-man.orc"
 */

object Roads extends CommandApp(

  name   = "road-changes",
  header = "How many kilometers of roads changed?",
  main   = {

    val orcO = Opts.option[String]("orc", help = "Location of the ORC file to process.")

    orcO.map { orc =>

      /* Settings compatible for both local and EMR execution */
      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("road-changes")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)

      implicit val ss: SparkSession = SparkSession.builder
        .config(conf)
        .enableHiveSupport
        .getOrCreate

      /* Silence the damn INFO logger */
      Logger.getRootLogger().setLevel(Level.ERROR)

      (Try(ss.read.orc(orc)) >>= Analysis.roads) match {
        case Failure(e) => println(e)
        case Success(d) => println(s"${d} kilometers of roads were changed.")
      }

      ss.stop()

    }
  }
)

object Analysis {

  /* The highway (road) types we care about. There are technically more than these. */
  val highways: Set[String] = Set(
    "motorway", "trunk", "motorway_link", "trunk_link", "primary", "secondary", "tertiary",
    "primary_link", "secondary_link", "tertiary_link", "service", "residential", "unclassified",
    "living_street", "road"
  )

  /** How many kilometers of road changed in all the Ways present in the given DataFrame? */
  def roads(data: DataFrame)(implicit ss: SparkSession): Try[Double] = {

    Try(osm.fromDataFrame(data)).map { case (nodes, ways, relations) =>
      val roadsOnly: RDD[(Long, osm.Way)] =
        ways.filter(_._2.data.tagMap.get("highway").map(highways.contains(_)).getOrElse(false))

      /* We expect this `lines` value to have more entries than `roadsOnly`,
       * since a new Line should be created for every Way change, but also for every
       * Node change in between.
       */
      val lines: RDD[Feature[Line, osm.ElementData]] = osm.toLines(nodes, roadsOnly)

      // println(s"ROADS: ${roadsOnly.count}")
      // println(s"LINES: ${lines.count}")

      // TODO You can probably be smarter and reassociate the Ways first.
      lines.aggregate(0d)({ _ + _.geom.length }, { _ + _ })
    }
  }

}
