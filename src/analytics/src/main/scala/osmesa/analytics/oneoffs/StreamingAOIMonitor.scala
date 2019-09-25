package osmesa.analytics.oneoffs

import cats.data.{Validated, ValidatedNel}
import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import geotrellis.vector._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, from_unixtime, to_timestamp, udf}
import org.locationtech.geomesa.spark.jts.{udf => _, _}
import org.locationtech.jts.{geom => jts}
import org.locationtech.jts.geom.prep._
import osmesa.analytics.Analytics
import vectorpipe.sources.Source

import java.net.URI

// These only required for test getAreaIndex implementation
import geotrellis.vector.io._
import geotrellis.vector.io.json._
import spray.json._

object StreamingAOIMonitor extends CommandApp(
  name = "streaming-aoi-monitor",
  header = "Streaming AOI Monitor",
  main = {
    val intervalOpt =
      Opts
        .option[String]("interval", help = "Period of time to aggregate over (d=daily, w=weekly, m=monthly)")
        .validate("Must be one of 'd', 'w', or 'm'") { arg: String => arg.length == 1 && "dwm".contains(arg.apply(0)) }

    val databaseUrlOpt =
      Opts
        .option[URI](
        "database-url",
        short = "d",
        metavar = "database URL",
        help = "URL of database containing AOI table (default: DATABASE_URL environment variable)"
      )
      .orElse(Opts.env[URI]("DATABASE_URL", help = "The URL of the database"))

    val augmentedDiffSourceOpt =
      Opts
        .option[URI](
        "augmented-diff-source",
        short = "a",
        metavar = "uri",
        help = "Location of augmented diffs to process"
      )

    val startSequenceOpt =
      Opts
        .option[Int](
        "start-sequence",
        short = "s",
        metavar = "sequence",
        help = "Starting sequence. If absent, the current (remote) sequence will be used."
      )
      .orNone

    val endSequenceOpt =
      Opts
        .option[Int](
        "end-sequence",
        short = "e",
        metavar = "sequence",
        help = "Ending sequence. If absent, this will be an infinite stream."
      )
      .orNone

    (intervalOpt, databaseUrlOpt, augmentedDiffSourceOpt, startSequenceOpt, endSequenceOpt).mapN {
      (interval, databaseUri, augmentedDiffSource, startSequence, endSequence) =>
      val appName = "StreamingAOIMonitor"

      implicit val spark: SparkSession = Analytics.sparkSession("Streaming AOI Monitor")

      import spark.implicits._
      spark.withJTS
      import AOIMonitorUtils._

      val aoiIndex = getAreaIndex(/*databaseUri*/)

      val options = Map(
        Source.BaseURI -> augmentedDiffSource.toString,
        Source.ProcessName -> appName
      ) ++
      startSequence
        .map(s => Map(Source.StartSequence -> s.toString))
        .getOrElse(Map.empty[String, String]) ++
      endSequence
        .map(s => Map(Source.EndSequence -> s.toString))
        .getOrElse(Map.empty[String, String])

      val diffs = spark
        .readStream
        .format(Source.AugmentedDiffs)
        .options(options)
        .load
        .withColumn("watermark", to_timestamp(from_unixtime('sequence * 60 + 1347432900)))
        .withWatermark("watermark", "0 seconds")

      val aoiTag = udf { g: jts.Geometry => aoiIndex(g).toList }

      val grouped = diffs.withColumn("aoi", explode(aoiTag('geom))).groupBy('aoi)

      // DO SOMETHING WITH THE DATA.
      // Aggregations will likely differ with
    }
  }
)

object AOIMonitorUtils {

  case class Country(name: String)

  object MyJsonProtocol extends DefaultJsonProtocol {
    implicit object CountryJsonFormat extends RootJsonFormat[Country] {
      def read(value: JsValue): Country =
        value.asJsObject.getFields("ENAME") match {
          case Seq(JsString(name)) =>
            Country(name)
          case v =>
            throw DeserializationException(s"Country expected, got $v")
        }

      def write(v: Country): JsValue =
        JsObject(
          "name" -> JsString(v.name)
        )
    }
  }

  import MyJsonProtocol._

  class AOIIndex(index: SpatialIndex[(PreparedGeometry, Country)]) extends Serializable {
    def apply(g: jts.Geometry): Traversable[Country] = {
      val t =
        new Traversable[(PreparedGeometry, Country)] {
          override def foreach[U](f: ((PreparedGeometry, Country)) => U): Unit = {
            val visitor = new org.locationtech.jts.index.ItemVisitor {
              override def visitItem(obj: AnyRef): Unit = f(obj.asInstanceOf[(PreparedGeometry, Country)])
            }
            index.rtree.query(Geometry(g).jtsGeom.getEnvelopeInternal, visitor)
          }
        }

      t.
        filter(_._1.intersects(g)).
        map(_._2)
    }
  }
  object AOIIndex {
    def apply(features: Seq[Feature[Geometry, Country]]): AOIIndex =
      new AOIIndex(
        SpatialIndex.fromExtents(
          features.map { mpf =>
            (PreparedGeometryFactory.prepare(mpf.geom.jtsGeom), mpf.data)
          }
        ) { case (pg, _) => pg.getGeometry.getEnvelopeInternal }
      )
  }

  def getAreaIndex(): AOIIndex = {
    val collection = vectorpipe.util.Resource("aois.geojson").parseGeoJson[JsonFeatureCollection]
    val polys = collection.getAllPolygonFeatures[Country].map(_.mapGeom(MultiPolygon(_)))
    val mps = collection.getAllMultiPolygonFeatures[Country]

    AOIIndex(polys ++ mps)
  }

  def getAreaIndex(databaseURI: URI): AOIIndex = {
    // Load a database table, grabbing names and geometries (decode geoms from WKB?)
    // Store in an AOIIndex

    ???
  }
}
