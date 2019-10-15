package osmesa.analytics.oneoffs

import java.net.URI
import java.sql.{Connection, DriverManager, Timestamp}
import java.util.UUID

import cats.data.Validated
import cats.implicits._
import com.monovore.decline.{Argument, CommandApp, Opts}
import geotrellis.vector._
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{collect_list, collect_set, count, explode, size, udf}
import org.locationtech.geomesa.spark.jts._
import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel}
import org.locationtech.jts.{geom => jts}
import org.locationtech.jts.geom.prep._
import org.locationtech.jts.io.WKBReader
import osmesa.analytics.Analytics
import osmesa.analytics.oneoffs.Interval._
import osmesa.analytics.stats._
import osmesa.analytics.stats.functions._
import vectorpipe.functions._
import vectorpipe.sources.Source

import scala.collection.mutable.ListBuffer
import scala.util.Properties

object StreamingAOIMonitor
    extends CommandApp(
      name = "streaming-aoi-monitor",
      header = "Streaming AOI Monitor",
      main = {
        val intervalOpt =
          Opts
            .option[Interval]("interval",
                              help = "Period of time to aggregate over (d=daily, w=weekly)")

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

        (intervalOpt, augmentedDiffSourceOpt, startSequenceOpt, endSequenceOpt).mapN {
          (interval, augmentedDiffSource, startSequence, endSequence) =>
            val appName = "StreamingAOIMonitor"

            implicit val spark: SparkSession = Analytics.sparkSession("Streaming AOI Monitor")

            import spark.implicits._
            spark.withJTS
            import AOIMonitorUtils._

            val notifications: List[Notification] = queryNotifications(interval)
            notifications.foreach { println(_) }
            val aoiIndex = AOIIndex(notifications)

            lazy val currentSequence = getCurrentSequence(augmentedDiffSource, interval).getOrElse(
              throw new RuntimeException(
                s"Could not pull current AugmentedDiff sequence from $augmentedDiffSource, and no alternative was provided")
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
                getLastSequence(interval) match {
                  case Some(seq) =>
                    seq
                  case None =>
                    interval match {
                      case Daily  => currentSequence - 1440
                      case Weekly => currentSequence - (1440 * 7)
                    }
                }
              }

            warnMessage(
              s"Running stream process from $startPosition to $endPosition in replication stream")

            // Lodge a warning message if we're processing a stream covering more than
            // 36 hours (8 days) for daily (weekly) interval
            (interval, endPosition - startPosition) match {
              case (Daily, diff)
                  if diff > (1.5 * 1440) && startSequence.isEmpty && endSequence.isEmpty =>
                warnMessage(
                  s"WHILE RUNNING DAILY UPDATE: catching up on too many days (${diff.toDouble / 1440}) of logs!")
              case (Weekly, diff) if diff > 11520 && startSequence.isEmpty && endSequence.isEmpty =>
                warnMessage(
                  s"WHILE RUNNING WEEKLY UPDATE: catching up on too many weeks (${diff.toDouble / 10080}) of logs!")
              case _ =>
                throw new IllegalArgumentException(
                  s"""Cannot process stream for interval "$interval" and sequence [$startPosition, $endPosition]""")
            }

            val options = Map(
              Source.BaseURI -> augmentedDiffSource.toString,
              Source.ProcessName -> appName,
              Source.StartSequence -> startPosition.toString
            ) ++
              endSequence
                .map(s => Map(Source.EndSequence -> s.toString))
                .getOrElse(Map.empty[String, String])

            val aoiTag = udf { g: jts.Geometry =>
              aoiIndex(g).toList
            }
            val notificationId = udf { n: NotificationData =>
              n.notificationId
            }

            // 1. READ IN DIFF STREAM
            //    This non-streaming process will grab a finite set of diffs, beginning
            //    with the starting sequence, and give a DataFrame.  Tag each diff with
            //    The set of participating aoi notifications.
            val diffs = spark.read
              .format(Source.AugmentedDiffs)
              .options(options)
              .load
              .withColumn("data", explode(aoiTag('geom)))
              .withColumn("notificationId", notificationId('data))

            // 2. EXTRACT SALIENT INFO FROM DIFFS
            //    Prepare a dataset of summaries, one for each AOI/user combo carrying
            //    the information we want to communicate in email.  Daily and weekly
            //    summaries will differ in terms of message content.
            val messageInfo: Dataset[NotificationSummary] = {
              val stats = diffs.withDelta
                .withColumn("measurements", DefaultMeasurements)
                .withColumn("counts", DefaultCounts)

              stats
                .groupBy('notificationId)
                .agg(
//                      sum_counts(collect_list('counts)) as 'counts,
//                      sum_measurements(collect_list('measurements)) as 'measurements,
                  count('id) as 'edit_count,
                  size(collect_set('changeset)) as 'changeset_count
                )
                .as[NotificationSummary]
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
            setBeginSequence(interval, endPosition)

            spark.stop
        }
      }
    )

object AOIMonitorUtils extends Logging {

  case class NotificationData(notificationId: UUID, userId: UUID, name: String, email: String)

  type Notification = Feature[Geometry, NotificationData]

  case class NotificationSummary(
      data: NotificationData,
      edit_count: Long,
      changeset_count: Int
  ) {
    def toMessageBody(): String = ???
  }

  class AOIIndex(index: SpatialIndex[(PreparedGeometry, NotificationData)]) extends Serializable {
    def apply(g: jts.Geometry): Traversable[NotificationData] = {
      val t =
        new Traversable[(PreparedGeometry, NotificationData)] {
          override def foreach[U](f: ((PreparedGeometry, NotificationData)) => U): Unit = {
            val visitor = new org.locationtech.jts.index.ItemVisitor {
              override def visitItem(obj: AnyRef): Unit =
                f(obj.asInstanceOf[(PreparedGeometry, NotificationData)])
            }
            index.rtree.query(Geometry(g).jtsGeom.getEnvelopeInternal, visitor)
          }
        }

      t.filter(_._1.intersects(g)).map(_._2)
    }
  }
  object AOIIndex {
    def apply(features: Seq[Notification]): AOIIndex =
      new AOIIndex(
        SpatialIndex.fromExtents(
          features.map { mpf =>
            (PreparedGeometryFactory.prepare(mpf.geom.jtsGeom), mpf.data)
          }
        ) { case (pg, _) => pg.getGeometry.getEnvelopeInternal }
      )
  }

  def getLastSequence(interval: Interval): Option[Int] = {
    queryBeginSequence(interval)
  }

  def getCurrentSequence(augmentedDiffSource: URI, interval: Interval)(
      implicit spark: SparkSession): Option[Int] = {
    spark.sparkContext
      .parallelize(Seq(augmentedDiffSource))
      .map { uri =>
        queryBeginSequence(interval)
      }
      .collect
      .apply(0)
  }

  def warnMessage: (=> String) => Unit = logWarning

  def queryBeginSequence(interval: Interval): Option[Int] = {
    var connection: Connection = null
    try {
      connection = AOIDatabaseConfig.getConnection
      val preppedStatement =
        connection.prepareStatement("SELECT sequence FROM checkpoint_interval WHERE interval = ?")
      preppedStatement.setString(1, interval.value)
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

  def setBeginSequence(interval: Interval, lastSequence: Int) = {
    var connection: Connection = null
    try {
      connection = AOIDatabaseConfig.getConnection
      val preppedStatement =
        connection.prepareStatement("INSERT INTO checkpoint_interval VALUES (?, ?)")
      preppedStatement.setString(1, interval.value)
      preppedStatement.setLong(2, lastSequence)
      preppedStatement.executeQuery()
    } catch {
      case e: Throwable => {
        warnMessage(s"ERROR in setBeginSequence: ${e.getMessage}")
      }
    } finally {
      if (connection != null) connection.close()
    }
  }

  def queryNotifications(interval: Interval): List[Notification] = {
    var connection: Connection = null
    val wkbReader = new WKBReader(new GeometryFactory(new PrecisionModel(), 4326))
    val data = ListBuffer[Notification]()
    try {
      connection = AOIDatabaseConfig.getConnection
      val now = new Timestamp(new java.util.Date().getTime)
      val query =
        """
          |SELECT
          |    n.id as notification_id,
          |    u.id as user_id,
          |    ST_AsBinary(n.geom) as geometry,
          |    n.name as name,
          |    u.email as email
          |FROM notification n
          |LEFT JOIN user_info u ON n.user_id = u.id
          |WHERE n.interval = ?::notification_interval AND
          |      (n.expires_at is null OR ? < n.expires_at)
          |""".stripMargin
      val preppedStatement =
        connection.prepareStatement(query)
      preppedStatement.setString(1, interval.value)
      preppedStatement.setTimestamp(2, now)
      val rs = preppedStatement.executeQuery()
      while (rs.next()) {
        val notificationId = UUID.fromString(rs.getString("notification_id"))
        val userId = UUID.fromString(rs.getString("user_id"))
        val geom = wkbReader.read(rs.getBytes("geometry"))
        val name = rs.getString("name")
        val email = rs.getString("email")
        val notification = Feature(Geometry(geom), NotificationData(notificationId, userId, name, email))
        data.prepend(notification)
      }
    } finally {
      if (connection != null) connection.close()
    }
    data.toList
  }
}

sealed trait Interval {
  def value: String
  def shortCode: String
}
object Interval {
  case object Weekly extends Interval {
    val value = "weekly"
    val shortCode = "w"
  }
  case object Daily extends Interval {
    val value = "daily"
    val shortCode = "d"
  }

  val options: Set[Interval] = Set(Daily, Weekly)

  def unapply(str: String): Option[Interval] = options.find(_.value == str)

  implicit val intervalArgument: Argument[Interval] = new Argument[Interval] {
    def read(str: String) = {
      options.find(_.shortCode == str) match {
        case Some(interval) => Validated.valid(interval)
        case None           => Validated.invalidNel(s"Invalid interval: $str")
      }
    }

    override def defaultMetavar: String = {
      val opts = options
        .map { i =>
          Seq(i.shortCode, i.value).mkString("=")
        }
        .mkString(", ")
      s"Period of time to aggregate over ($opts)"
    }
  }
}

object AOIDatabaseConfig {
  val jdbcHost: String =
    Properties.envOrElse("POSTGRES_HOST", "localhost")

  val jdbcPort: String =
    Properties.envOrElse("POSTGRES_PORT", "5432")

  val jdbcNoDBUrl: String =
    Properties.envOrElse("POSTGRES_URL", s"jdbc:postgresql://$jdbcHost:$jdbcPort/")

  val jdbcDBName: String =
    Properties.envOrElse("POSTGRES_NAME", "aoi-monitoring")

  val dbUser: String = Properties.envOrElse("POSTGRES_USER", "aoi-monitoring")

  val dbPassword: String =
    Properties.envOrElse("POSTGRES_PASSWORD", "aoi-monitoring")

  val jdbcUrl: String = s"$jdbcNoDBUrl$jdbcDBName"

  def getConnection(): Connection = {
    DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)
  }
}
