package osmesa.analytics.oneoffs

import cats.data.{Validated, ValidatedNel}
import cats.implicits._
import com.monovore.decline._
import io.circe.generic.auto._
import io.circe.{yaml, _}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import osmesa.analytics.Analytics
import vectorpipe.sources.{ChangesetSource, Source}

import java.net.URI
import java.sql.Timestamp
import java.time.Instant
import scalaj.http.Http

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.ChangesetORCUpdater \
 *   ingest/target/scala-2.11/osmesa-analytics.jar \
 *   --changeset-source http://somewhere/diffs/ \
 *   --database-url $DATABASE_URL
 */
object ChangesetORCUpdater
  extends CommandApp(
    name = "osmesa-changeset-stats-updater",
    header = "Update statistics from augmented diffs",
    main = {

      import ChangesetORCUpdaterUtils._

      val changesetSourceOpt =
        Opts
          .option[URI](
          "changesets",
          short = "c",
          metavar = "uri",
          help = "Location of replication changesets"
        )
        .validate("Changeset source must have trailing '/'") { _.getPath.endsWith("/") }

      val endTimeOpt =
        Opts
          .option[Instant]("end-time",
                       short = "e",
                       metavar = "timestamp",
                       help = "Timestamp of stream end (of the form 2016-02-29T13:45:00Z); if absent, the time now will be used")
                         .orNone

      val orcArg = Opts
        .argument[URI]("source ORC")
        .validate("URI to ORC must have an s3 or file scheme") { _.getScheme != null }
        .validate("orc must be an S3 or file Uri") { uri =>
          uri.getScheme.startsWith("s3") || uri.getScheme.startsWith("file")
        }
        .validate("orc must be an .orc file") { _.getPath.endsWith(".orc") }

      val outputArg = Opts.argument[URI]("destination ORC")
        .validate("Output URI must have a scheme") { _.getScheme != null }
        .validate("Output URI must have an S3 or file scheme") { uri =>
          uri.getScheme.startsWith("s3") || uri.getScheme.startsWith("file")
        }
        .validate("orc must be an .orc file") { _.getPath.endsWith(".orc") }

      (changesetSourceOpt,
       endTimeOpt,
       orcArg,
       outputArg).mapN {
        (changesetSource, endTime, orcUri, outputURI) =>
        implicit val spark: SparkSession = Analytics.sparkSession("ChangesetStatsUpdater")

        import spark.implicits._

        val df = spark.read.orc(orcUri.toString)
        val lastModified = df.select(max(coalesce('closed_at, 'created_at))).first.getAs[Timestamp](0)

        val startSequence = findSequenceFor(lastModified.toInstant, changesetSource)
        val endSequence = endTime.map(findSequenceFor(_, changesetSource)).getOrElse(getCurrentSequence(changesetSource).sequence)

        val options = Map(
          Source.BaseURI -> changesetSource.toString,
          Source.StartSequence -> startSequence.toString,
          Source.EndSequence -> (endSequence + 1).toString // sequence range is (]; end sequence is exclusive
        )

        val changesets = spark.read.format(Source.Changesets).options(options).load
        changesets
          .repartition(1)
          .write
          .orc(outputURI.toString)

        spark.stop()
      }
    }
)

object ChangesetORCUpdaterUtils {
  implicit val readInstant: Argument[Instant] = new Argument[Instant] {
    override def read(string: String): ValidatedNel[String, Instant] = {
      try { Validated.valid(Instant.parse(string)) }
      catch { case e: Exception => Validated.invalidNel(s"Invalid time: $string (${ e.getMessage })") }
    }

    override def defaultMetavar: String = "time"
  }

  // implicit val TimestampFormat : Decoder[Timestamp] = new Decoder[Timestamp] {
  //   private val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss XXX")

  //   override def apply(c: HCursor): Decoder.Result[Timestamp] =
  //     c.get[String]("last_run").map{ str =>
  //       val splt = str.split(".")
  //       Timestamp.from(sdf.parse(splt(0) ++ splt(1).split(" ")(1)).toInstant)
  //     }
  // }

  private val formatter = DateTimeFormat.forPattern("y-M-d H:m:s.SSSSSSSSS Z")

  private implicit val dateTimeDecoder: Decoder[DateTime] =
    Decoder.instance(a => a.as[String].map(DateTime.parse(_, formatter)))

  case class Sequence(last_run: DateTime, sequence: Long)

  def getCurrentSequence(baseURI: URI): Sequence = {
    val response =
      Http(baseURI.resolve("state.yaml").toString).asString

    val state = yaml.parser
      .parse(response.body)
      .leftMap(err => err: Error)
      .flatMap(_.as[Sequence])
      .valueOr(throw _)

    state
  }

  def getSequence(baseURI: URI, sequence: Long): Sequence = {
    val s = f"${sequence+1}%09d"
    val path = s"${s.slice(0, 3)}/${s.slice(3, 6)}/${s.slice(6, 9)}.state.txt"

    val response =
      Http(baseURI.resolve(path).toString).asString

    val state = yaml.parser
      .parse(response.body)
      .leftMap(err => err: Error)
      .flatMap(_.as[Sequence])
      .valueOr(throw _)

    state
  }

  def estimateSequenceNumber(modifiedTime: Instant, baseURI: URI): Long = {
    val current = getCurrentSequence(baseURI)
    val diffMinutes = (current.last_run.toInstant.getMillis/1000 - modifiedTime.getEpochSecond) / 60
    current.sequence - diffMinutes
  }

  def findSequenceFor(modifiedTime: Instant, baseURI: URI): Long = {
    var guess = estimateSequenceNumber(modifiedTime, baseURI)
    val target = org.joda.time.Instant.parse(modifiedTime.toString)

    while (getSequence(baseURI, guess).last_run.isAfter(target)) { guess -= 1 }
    while (getSequence(baseURI, guess).last_run.isBefore(target)) { guess += 1 }

    getSequence(baseURI, guess).sequence
  }
}
