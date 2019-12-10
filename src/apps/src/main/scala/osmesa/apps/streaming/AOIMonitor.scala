package osmesa.apps.streaming

import java.net.URI
import java.sql.{Connection, DriverManager, Timestamp}
import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneId, ZonedDateTime}

import cats.data.Validated
import cats.implicits._
import com.monovore.decline.{Argument, CommandApp, Opts}
import geotrellis.vector._
import javax.mail.internet.InternetAddress
import org.apache.commons.mail._
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{collect_set, count, explode, size, udf}
import org.locationtech.geomesa.spark.jts._
import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel}
import org.locationtech.jts.{geom => jts}
import org.locationtech.jts.geom.prep._
import org.locationtech.jts.io.WKBReader
import osmesa.analytics.Analytics
import osmesa.analytics.stats._
import osmesa.apps.streaming.Interval._
import vectorpipe.sources.{AugmentedDiffSource, Source}

import collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Properties

object AOIMonitor
    extends CommandApp(
      name = "streaming-aoi-monitor",
      header = "Streaming AOI Monitor",
      main = {
        val intervalOpt =
          Opts
            .option[Interval](
              "interval",
              help =
                "Period of time to aggregate over (d=daily, w=weekly). Can also be provided via AOI_INTERVAL environment variable. Default: daily.")
            .orNone

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
              help =
                "Ending sequence. If absent, this will default to the sequence for Instant.now()."
            )
            .orNone

        (intervalOpt, augmentedDiffSourceOpt, startSequenceOpt, endSequenceOpt).mapN {
          (intervalArg, augmentedDiffSource, startSequence, endSequence) =>
            val appName = "StreamingAOIMonitor"
            val now = Timestamp.from(Instant.now)

            implicit val spark: SparkSession = Analytics.sparkSession("Streaming AOI Monitor")

            import spark.implicits._
            import AOIMonitorUtils._

            val defaultInterval: Interval = Daily
            val interval: Interval = intervalArg match {
              case Some(i) => i
              case None =>
                Interval
                  .unapply(Properties.envOrElse("AOI_INTERVAL", ""))
                  .getOrElse(defaultInterval)
            }

            val notificationList: List[Notification] = queryNotifications(interval)
            val notifications = spark.createDataset(notificationList)
            val aoiIndex = spark.sparkContext.broadcast(AOIIndex(notificationList))

            lazy val currentSequence = AugmentedDiffSource
              .getCurrentSequence(augmentedDiffSource)
              // Include small offset so that we're not processing the sequence for exactly now
              .getOrElse(AugmentedDiffSource.timestampToSequence(now) - 10)

            val endPosition =
              if (endSequence isDefined)
                endSequence.get
              else
                currentSequence
            val endTimestamp = AugmentedDiffSource.sequenceToTimestamp(endPosition)

            val startPosition =
              if (startSequence isDefined)
                startSequence.get
              else {
                getLastSequence(interval) match {
                  case Some(seq) =>
                    seq
                  case None =>
                    interval match {
                      case Daily  => endPosition - 1440
                      case Weekly => endPosition - (1440 * 7)
                    }
                }
              }
            val startTimestamp = AugmentedDiffSource.sequenceToTimestamp(startPosition)

            val positionMessage =
              s"""
                | Running stream process from:
                |   $startPosition ($startTimestamp)
                | to:
                |   $endPosition ($endTimestamp)
                | in replication stream
                |""".stripMargin
            warnMessage(positionMessage)

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
                warnMessage(
                  s"""Processing stream for interval "$interval" and sequence [$startPosition, $endPosition]""")
            }

            val options = Map(
              Source.BaseURI -> augmentedDiffSource.toString,
              Source.ProcessName -> appName,
              Source.StartSequence -> startPosition.toString,
              Source.EndSequence -> endPosition.toString
            )

            val aoiTag = udf { g: jts.Geometry =>
              aoiIndex.value(g).toList
            }

            // 1. READ IN DIFF STREAM
            //    This non-streaming process will grab a finite set of diffs, beginning
            //    with the starting sequence, and give a DataFrame.  Tag each diff with
            //    the set of participating aoi notifications.
            val diffs = spark.read
              .format(Source.AugmentedDiffs)
              .options(options)
              .load
              // Given Diff geom, return exploded list of matching notificationIds
              .withColumn("notificationId", explode(aoiTag('geom)))

            // 2. EXTRACT SALIENT INFO FROM DIFFS
            //    Prepare a dataset of summaries, one for each notification to send.
            val changeSummary = {
              val stats = diffs.withDelta
                .withColumn("osmUser", 'user)
                .drop('user)

              stats
                .groupBy('notificationId, 'osmUser)
                .agg(
                  count('id) as 'editCount,
                  size(collect_set('changeset)) as 'changesetCount
                )
                .orderBy('editCount.desc)
                .as[UserChangeSummary]
            }
            val groupedChanges = changeSummary.groupByKey(_.notificationId)
            val notificationsByKey = notifications.groupByKey(_.notificationId)
            val messageInfo: Dataset[NotificationSummary] =
              groupedChanges.cogroup(notificationsByKey) {
                case (notificationId: String,
                      changes: Iterator[UserChangeSummary],
                      notifications: Iterator[Notification]) => {
                  if (changes.hasNext && notifications.hasNext) {
                    val data = notifications.next
                    Some(
                      NotificationSummary(notificationId,
                                          data.userId,
                                          data.aoi,
                                          data.name,
                                          data.email,
                                          changes.toSeq))
                  } else {
                    None
                  }
                }
                case _ => None
              }

            // 4. SEND MESSAGES TO QUEUE
            //    We need to craft an email from each record and queue it for sending
            val aoiInterval = AOIInterval(startTimestamp, endTimestamp, interval)
            val bAoiInterval = spark.sparkContext.broadcast(aoiInterval)
            val sendResult: NotificationSendResult = messageInfo.map {
              info =>
                val subject =
                  s"${interval.value.capitalize} AOI Summary for ${info.name} ending $endTimestamp"
                val message = info.toMessageBody(bAoiInterval.value)
                val fromAddress = AOIEmailConfig.fromAddress
                if (!fromAddress.isEmpty) {
                  val toAddress = new InternetAddress(info.email)
                  val email = new SimpleEmail()
                  email.setHostName(AOIEmailConfig.smtpHostname)
                  email.setSmtpPort(AOIEmailConfig.smtpPort)

                  email.setFrom(AOIEmailConfig.fromAddress)
                  email.setTo(Seq(toAddress).asJavaCollection)
                  email.setSubject(subject)
                  email.setMsg(message)
                  try {
                    email.send
                    NotificationSendResult(1, Array.empty[NotificationSendError])
                  } catch {
                    case error: Throwable => {
                      val msg =
                        s"""
                          |ERROR Unable to send message for notification ${info.notificationId}
                          |Message: ${error.getLocalizedMessage}
                          |Trace:
                          |${error.getStackTrace.mkString("\n")}
                          |""".stripMargin
                      errorMessage(msg)
                      NotificationSendResult(
                        0,
                        Array(NotificationSendError(info.notificationId, error.getLocalizedMessage))
                      )
                    }
                  }
                } else {
                  warnMessage(s"Sending Notification for ${info.notificationId}:\n$message")
                  NotificationSendResult(1, Array.empty[NotificationSendError])
                }
            }.reduce(_ combine _)
            sendAdminStatusMessage(aoiInterval, sendResult)

            // 5. SAVE CURRENT END POSITION IN DB FOR NEXT RUN
            val setBeginResult = setBeginSequence(interval, endPosition)
            if (setBeginResult > 0) {
              warnMessage(
                s"checkpoint_interval set: (${interval.value}, $endPosition, $endTimestamp)")
            }

            spark.stop
        }
      }
    )

object AOIMonitorUtils extends Logging {

  case class AOIInterval(start: Timestamp, end: Timestamp, interval: Interval)

  case class Notification(notificationId: String,
                          userId: String,
                          aoi: jts.Geometry,
                          name: String,
                          email: String)

  case class UserChangeSummary(notificationId: String,
                               osmUser: String,
                               editCount: Long,
                               changesetCount: Int)

  case class NotificationSummary(
      notificationId: String,
      userId: String,
      geom: jts.Geometry,
      name: String,
      email: String,
      changes: Seq[UserChangeSummary]
  ) {
    def toMessageBody(aoiInterval: AOIInterval): String = {
      val messageUserList = changes
        .map { ucs =>
          s"   - ${ucs.osmUser}: ${ucs.editCount} edits, ${ucs.changesetCount} changesets"
        }
        .mkString("\n")

      s"""
        |
        | AOI Notification for: $name
        |
        | Summary by user:
        |
        |$messageUserList
        |
        | for the ${aoiInterval.interval.value} interval from ${aoiInterval.start} to ${aoiInterval.end}.
        |
        |""".stripMargin
    }
  }

  case class NotificationSendError(notificationId: String, message: String)

  case class NotificationSendResult(successCount: Int, errors: Array[NotificationSendError]) {
    def combine(other: NotificationSendResult): NotificationSendResult = {
      NotificationSendResult(successCount + other.successCount, errors ++ other.errors)
    }
  }

  class AOIIndex(index: SpatialIndex[(PreparedGeometry, String)]) extends Serializable {
    def apply(g: jts.Geometry): Traversable[String] = {
      val t =
        new Traversable[(PreparedGeometry, String)] {
          override def foreach[U](f: ((PreparedGeometry, String)) => U): Unit = {
            val visitor = new org.locationtech.jts.index.ItemVisitor {
              override def visitItem(obj: AnyRef): Unit =
                f(obj.asInstanceOf[(PreparedGeometry, String)])
            }
            index.rtree.query(g.getEnvelopeInternal, visitor)
          }
        }
      t.filter(_._1.intersects(g)).map(_._2)
    }
  }
  object AOIIndex {
    def apply(notifications: Seq[Notification]): AOIIndex =
      new AOIIndex(
        SpatialIndex.fromExtents(
          notifications.map { n =>
            (PreparedGeometryFactory.prepare(n.aoi), n.notificationId)
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
        AugmentedDiffSource.getCurrentSequence(uri)
      }
      .collect
      .apply(0)
  }

  def errorMessage: (=> String) => Unit = logError
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

  def setBeginSequence(interval: Interval, lastSequence: Int): Int = {
    var connection: Connection = null
    try {
      connection = AOIDatabaseConfig.getConnection
      val sql =
        """
          |INSERT INTO checkpoint_interval (interval, sequence)
          |VALUES (?, ?)
          |ON CONFLICT (interval)
          |DO UPDATE SET sequence = ?
          |""".stripMargin
      val preppedStatement = connection.prepareStatement(sql)
      preppedStatement.setString(1, interval.value)
      preppedStatement.setLong(2, lastSequence)
      preppedStatement.setLong(3, lastSequence)
      preppedStatement.executeUpdate()
    } catch {
      case e: Throwable => {
        errorMessage(s"ERROR in setBeginSequence: ${e.getMessage}")
        0
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
        val notificationId = rs.getString("notification_id")
        val userId = rs.getString("user_id")
        val geom = wkbReader.read(rs.getBytes("geometry"))
        val name = rs.getString("name")
        val email = rs.getString("email")
        val notification = Notification(notificationId, userId, geom, name, email)
        data.prepend(notification)
      }
    } finally {
      if (connection != null) connection.close()
    }
    data.toList
  }

  def sendAdminStatusMessage(aoiInterval: AOIInterval, sendResult: NotificationSendResult): Unit = {
    val errorCount = sendResult.errors.size
    var errorString = sendResult.errors.map { e =>
      s"  - ${e.notificationId}: ${e.message}"
    }.mkString("\n")
    if (errorString.isEmpty) {
      errorString = "NONE"
    }

    val adminEmails = AOIEmailConfig.adminEmails
    val intervalText = aoiInterval.interval.value.capitalize
    val subject = s"$errorCount Errors: $intervalText AOI Notification"
    val message =
      s"""
        |Sent ${sendResult.successCount} email notifications for the $intervalText interval:
        |
        |${aoiInterval.start} to ${aoiInterval.end}
        |
        |The following errors were reported:
        |$errorString
        |
        |""".stripMargin
    adminEmails.foreach { toAddressString =>
      val toAddress = new InternetAddress(toAddressString)
      val email = new SimpleEmail()
      email.setHostName(AOIEmailConfig.smtpHostname)
      email.setSmtpPort(AOIEmailConfig.smtpPort)

      email.setFrom(AOIEmailConfig.fromAddress)
      email.setTo(Seq(toAddress).asJavaCollection)
      email.setSubject(subject)
      email.setMsg(message)
      try {
        email.send
      } catch {
        case error: Throwable => {
          val message =
            s"""
               |ERROR Unable to send admin status message to ${toAddressString}
               |Message: ${error.getLocalizedMessage}
               |Trace:
               |${error.getStackTrace.mkString("\n")}
               |""".stripMargin
          errorMessage(message)
        }
      }
    }
  }
}

sealed trait Interval {
  def value: String
  def shortCode: String
  def chronoUnit: ChronoUnit
  def subtractFrom(time: Timestamp): Timestamp = {
    val utcZone = ZoneId.of("Etc/UTC")
    val timeZdt = ZonedDateTime.ofInstant(time.toInstant, utcZone)
    val startZdt = timeZdt.minus(1, chronoUnit)
    Timestamp.from(startZdt.toInstant)
  }
}
object Interval {
  case object Weekly extends Interval {
    val value = "weekly"
    val shortCode = "w"
    val chronoUnit = ChronoUnit.WEEKS
  }
  case object Daily extends Interval {
    val value = "daily"
    val shortCode = "d"
    val chronoUnit = ChronoUnit.DAYS
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

object AOIEmailConfig {
  val smtpHostname: String = Properties.envOrElse("AOI_SMTP_HOSTNAME", "localhost")
  val smtpPort: Int = Properties.envOrElse("AOI_SMTP_PORT", "25").toInt
  val fromAddress: String = Properties.envOrElse("AOI_FROM_ADDRESS", "")

  // Provide as a comma-separated string of valid email addresses
  val adminEmails: Seq[String] = Properties.envOrElse("AOI_ADMIN_EMAILS", "").trim.split(",")
}