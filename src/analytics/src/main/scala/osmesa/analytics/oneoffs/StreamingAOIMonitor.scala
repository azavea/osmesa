package osmesa.analytics.oneoffs

import cats.data.{Validated, ValidatedNel}
import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import geotrellis.vector._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{collect_list, collect_set, count, explode, size, udf}
import org.locationtech.geomesa.spark.jts.{udf => _, _}
import org.locationtech.jts.{geom => jts}
import org.locationtech.jts.geom.prep._
import osmesa.analytics.Analytics
import osmesa.analytics.stats._
import osmesa.analytics.stats.functions._
import vectorpipe.functions._
import vectorpipe.sources.{AugmentedDiffSource, Source}
import vectorpipe.util.DBUtils

import java.net.URI
import java.sql._

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
        .option[String]("interval", help = "Period of time to aggregate over (d=daily, w=weekly)")
        .validate("Must be one of 'd', 'w'") { arg: String => arg.length == 1 && "dw".contains(arg.apply(0)) }

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

      val procName = appName ++ "_" ++ interval

      lazy val currentSequence = getCurrentSequence(augmentedDiffSource, procName).getOrElse(
        throw new RuntimeException(s"Could not pull current AugmentedDiff sequence from $augmentedDiffSource, and no alternative was provided")
      )

      val endPosition =
        if (endSequence isDefined)
          endSequence.get
        else
          currentSequence

      val startPosition =
        if (startSequence isDefined)
          startSequence.get
        else {
          getLastSequence(databaseUri, interval) match {
            case Some(seq) =>
              warnMessage(s"Successfully read last sequence number from $databaseUri")
              seq
            case None =>
              interval match {
                case "d" => currentSequence - 1440
                case "w" => currentSequence - (1440 * 7)
              }
          }
        }

      warnMessage(s"Running stream process from $startPosition to $endPosition in replication stream")

      // Lodge a warning message if we're processing a stream covering more than
      // 36 hours (8 days) for daily (weekly) interval
      (interval, endPosition - startPosition) match {
        case ("d", diff) if diff > (1.5 * 1440) && startSequence.isEmpty && endSequence.isEmpty =>
          warnMessage(s"WHILE RUNNING DAILY UPDATE: catching up on too many days (${diff.toDouble/1440}) of logs!")
        case ("w", diff) if diff > 11520 && startSequence.isEmpty && endSequence.isEmpty =>
          warnMessage(s"WHILE RUNNING WEEKLY UPDATE: catching up on too many weeks (${diff.toDouble/10080}) of logs!")
        case _ =>
          throw new IllegalArgumentException(s"""Cannot process stream for interval "$interval" and sequence [$startPosition, $endPosition]""")
      }

      val options = Map(
        Source.BaseURI -> augmentedDiffSource.toString,
        Source.ProcessName -> appName,
        Source.StartSequence -> startPosition.toString
      ) ++
      endSequence
        .map(s => Map(Source.EndSequence -> s.toString))
        .getOrElse(Map.empty[String, String])

      val aoiIndex = getAreaIndex(/*databaseUri*/)
      val aoiTag = udf { g: jts.Geometry => aoiIndex(g).toList }

      // 1. READ IN DIFF STREAM
      //    This non-streaming process will grab a finite set of diffs, beginning
      //    with the starting sequence, and give a DataFrame.  Tag each diff with
      //    The set of participating AOIs.
      val diffs = spark
        .read
        .format(Source.AugmentedDiffs)
        .options(options)
        .load
        .withColumn("aoi", explode(aoiTag('geom)))

      // 2. EXTRACT SALIENT INFO FROM DIFFS
      //    Prepare a dataset of summaries, one for each AOI/user combo carrying
      //    the information we want to communicate in email.  Daily and weekly
      //    summaries will differ in terms of message content.

      // NOTE: The following does not compile because Dataset[T] is not invariant
      // over T.  We will need to convert to some common representation (say, the
      // email message itself).
      val messageInfo: Dataset[AOIUserSummary] =
        interval match {
          case "d" =>
            val stats = diffs
              .withDelta
              .withColumn("measurements", DefaultMeasurements)
              .withColumn("counts", DefaultCounts)

            stats
              .groupBy('aoi, 'uid)
              .agg(
                sum_counts(collect_list('counts)) as 'counts,
                sum_measurements(collect_list('measurements)) as 'measurements,
                count('id) as 'edit_count,
                size(collect_set('changeset)) as 'changeset_count
              )
              .as[DailyInfo]
          case "w" =>
            diffs
              .groupBy('aoi, 'uid)
              .agg(
                count('id) as 'edit_count,
                size(collect_set('changeset)) as 'changeset_count
              )
              .as[WeeklyInfo]
        }

      // 3. CONSTRUCT LOOKUP TABLE FOR AOI INFO
      //    We need to package up the information about AOIs (specifically the
      //    name and subscriber list) so that we may associate that with each info
      //    message and send the email.  This should be a map?  Or is this a
      //    separate DataFrame that we join to `diffs` before step 2?

      // 4. SEND MESSAGES TO QUEUE
      //    We need to craft an email from each record and queue it for sending
      //    via SES.
      messageInfo.foreach { info =>

      }

      // 5. SAVE CURRENT END POSITION IN DB FOR NEXT RUN

      spark.stop
    }
  }
)

object AOIMonitorUtils extends Logging{

  sealed trait AOIUserSummary {
    def toMessageBody(): String
  }

  case class DailyInfo(
    aoi: Int,
    uid: Long,
    counts: Map[String, Int],
    measurements: Map[String, Double],
    edit_count: Long,
    changeset_count: Int
  ) extends AOIUserSummary {
    def toMessageBody(): String = ???
  }

  case class WeeklyInfo(
    aoi: Int,
    uid: Long,
    edit_count: Long,
    changeset_count: Int
  ) extends AOIUserSummary {
    def toMessageBody(): String = ???
  }


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

  def queryLastSequence(databaseURI: URI, procName: String): Option[Int] = {
    var connection: Connection = null
    try {
      connection = DBUtils.getJdbcConnection(databaseURI)
      val preppedStatement =
        connection.prepareStatement("SELECT sequence FROM checkpoints WHERE proc_name = ?")
      preppedStatement.setString(1, procName)
      val rs = preppedStatement.executeQuery()
      if (rs.next()) {
        rs.getInt("sequence") match {
          case 0 => None
          // sequence was checkpointed after completion; start with the next one
          case seq => Some(seq + 1)
        }
      } else {
        None
      }
    } finally {
      if (connection != null) connection.close()
    }
  }

  def getLastSequence(databaseURI: URI, procName: String)(implicit spark: SparkSession): Option[Int] = {
    spark.sparkContext.parallelize(Seq(databaseURI)).map { uri =>
      queryLastSequence(uri, procName)
    }.collect.apply(0)
  }

  def getCurrentSequence(augmentedDiffSource: URI, procName: String)(implicit spark: SparkSession): Option[Int] = {
    spark.sparkContext.parallelize(Seq(augmentedDiffSource)).map { uri =>
      queryLastSequence(uri, procName)
    }.collect.apply(0)
  }

  def warnMessage: (=> String) => Unit = logWarning
}
