package osmesa.apps.batch

import java.net.URI
import java.sql._
import java.time.Instant

import cats.data.{Validated, ValidatedNel}
import cats.implicits._
import com.monovore.decline._
import io.circe._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import osmesa.analytics.Analytics
import vectorpipe.sources.{ChangesetSource, Source}
import vectorpipe.util.DBUtils

/*
 * Usage example:
 *
 * sbt "project apps" assembly
 *
 * spark-submit \
 *   --class osmesa.apps.batch.MergeChangesets \
 *   ingest/target/scala-2.11/osmesa-apps.jar \
 *   --changesets http://location/of/changeset/replications \
 *   --end-time 1970-01-01T13:00:00Z
 *   s3://path/to/history.orc
 *   s3://path/to/output.orc
 */
object MergeChangesets
  extends CommandApp(
    name = "osmesa-merge-changesets",
    header = "Bring existing changeset ORC file up to date using changeset stream",
    main = {

      import ChangesetSource._
      import MergeChangesetsImplicits._

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
        implicit val spark: SparkSession = Analytics.sparkSession("MergeChangesets")

        import spark.implicits._

        val df = spark.read.orc(orcUri.toString)
        val lastModified = df.select(max(coalesce('closed_at, 'created_at))).first.getAs[Timestamp](0)

        val startSequence = findSequenceFor(lastModified.toInstant, changesetSource)
        val endSequence = endTime.map(findSequenceFor(_, changesetSource)).getOrElse(getCurrentSequence(changesetSource).get.sequence)

        val options = Map(
          Source.BaseURI -> changesetSource.toString,
          Source.StartSequence -> startSequence.toString,
          Source.EndSequence -> (endSequence + 1).toString // sequence range is (]; end sequence is exclusive
        )

        val changesets = spark.read.format(Source.Changesets).options(options).load

        // TODO: Clean up the following by providing and using a function in VP to coerce the
        // column names into camel case (see https://github.com/geotrellis/vectorpipe/issues/113)
        changesets
          .drop("comments", "sequence")
          .union(df.select(
            'id,
            'tags,
            'created_at as 'createdAt,
            'open,
            'closed_at as 'closedAt,
            'comments_count as 'commentsCount,
            'min_lat as 'minLat,
            'max_lat as 'maxLat,
            'min_lon as 'minLon,
            'max_lon as 'maxLon,
            'num_changes as 'numChanges,
            'uid,
            'user)
          )
          .repartition(1)
          .write
          .orc(outputURI.toString)

        spark.stop()
      }
    }
)
object MergeChangesetsImplicits {
  implicit val readInstant: Argument[Instant] = new Argument[Instant] {
    override def read(string: String): ValidatedNel[String, Instant] = {
      try { Validated.valid(Instant.parse(string)) }
      catch { case e: Exception => Validated.invalidNel(s"Invalid time: $string (${ e.getMessage })") }
    }

    override def defaultMetavar: String = "time"
  }

  private val formatter = DateTimeFormat.forPattern("y-M-d H:m:s.SSSSSSSSS Z")

  private implicit val dateTimeDecoder: Decoder[DateTime] =
    Decoder.instance(a => a.as[String].map(DateTime.parse(_, formatter)))
}
