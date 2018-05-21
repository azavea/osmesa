package osmesa.analytics.oneoffs

import java.io.ByteArrayInputStream
import java.net.URI
import java.util.zip.GZIPInputStream

import cats.implicits._
import com.monovore.decline._
import geotrellis.vector.{Feature, Geometry}
import io.circe.generic.auto._
import io.circe.{yaml, _}
import org.apache.commons.io.IOUtils
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import osmesa.common.AugmentedDiff
import scalaj.http.Http

import scala.xml.XML

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
            .withDefault(
              new URI("https://planet.osm.org/replication/changesets/"))
        val databaseUrlOpt = Opts
          .option[URI]("database-url",
                       short = "d",
                       metavar = "database URL",
                       help = "Database URL")
          .orNone

        (changesetSourceOpt, databaseUrlOpt).mapN {
          (changesetSource, databaseUri) =>
            /* Settings compatible for both local and EMR execution */
            val conf = new SparkConf()
              .setIfMissing("spark.master", "local[*]")
              .setAppName("changeset-stream-processor")
              .set("spark.serializer",
                   classOf[org.apache.spark.serializer.KryoSerializer].getName)
              .set("spark.kryo.registrator",
                   classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)

            implicit val ss: SparkSession = SparkSession.builder
              .config(conf)
              .enableHiveSupport
              .getOrCreate

            // state.yaml uses a custom date format
            val formatter = DateTimeFormat.forPattern("y-M-d H:m:s.SSSSSSSSS Z")
            implicit val dateTimeDecoder: Decoder[DateTime] = Decoder.instance(
              a => a.as[String].map(DateTime.parse(_, formatter)))
            case class ChangesetsState(last_run: DateTime, sequence: Int)

            // TODO more of these should be optional
            class Changeset(val id: Long,
                            val createdAt: DateTime,
                            val closedAt: Option[DateTime],
                            val open: Boolean,
                            val numChanges: Int,
                            val user: String,
                            val uid: Long,
                            val minLat: Option[Float],
                            val maxLat: Option[Float],
                            val minLon: Option[Float],
                            val maxLon: Option[Float],
                            val commentsCount: Int,
                            val tags: Map[String, String])

            implicit def stringToDateTime(s: String): DateTime =
              DateTime.parse(s)

            implicit def stringToOptionalDateTime(s: String): Option[DateTime] =
              s match {
                case "" => None
                case ts => Some(ts)
              }

            implicit def stringToOptionalFloat(s: String): Option[Float] =
              s match {
                case "" => None
                case c  => Some(c.toFloat)
              }

            object Changeset {
              def fromXML(node: scala.xml.Node): Changeset = {
                val id = (node \@ "id").toLong
                val commentsCount = (node \@ "comments_count").toInt
                val uid = (node \@ "uid").toLong
                val user = node \@ "user"
                val numChanges = (node \@ "num_changes").toInt
                val open = (node \@ "open").toBoolean
                val closedAt = node \@ "closed_at"
                val createdAt = node \@ "created_at"

                val maxLon = node \@ "max_lon"
                val minLon = node \@ "min_lon"
                val maxLat = node \@ "max_lon"
                val minLat = node \@ "min_lon"
                val tags =
                  (node \ "tag").map(tag => (tag \@ "k", tag \@ "v")).toMap

                new Changeset(id,
                              createdAt,
                              closedAt,
                              open,
                              numChanges,
                              user,
                              uid,
                              minLat,
                              maxLat,
                              minLon,
                              maxLon,
                              commentsCount,
                              tags)
              }
            }

            class ChangesetsReceiver(
                initialSequence: Option[Int],
                baseUri: URI =
                  new URI("https://planet.osm.org/replication/changesets/"))
                extends Receiver[Changeset](StorageLevel.MEMORY_AND_DISK_2)
                with Logging {
              private var sequence = initialSequence match {
                case Some(s) => s
                case None =>
                  try {
                    val response =
                      Http(changesetSource.resolve("state.yaml").toString).asString

                    val state = yaml.parser
                      .parse(response.body)
                      .leftMap(err => err: Error)
                      .flatMap(_.as[ChangesetsState])
                      .valueOr(throw _)

                    println(
                      s"Remote state: ${state.sequence} @ ${state.last_run}")

                    state.sequence
                  } catch {
                    case t: Throwable =>
                      println(
                        s"Error reading current state from $changesetSource",
                        t)
                      throw t
                  }
              }

              def onStart(): Unit = {
                new Thread("Changesets receiver") {
                  override def run(): Unit = {
                    receive()
                  }
                }.start()
              }

              def onStop(): Unit = {}

              private def receive(): Unit = {
                // TODO run this in a loop rather than calling restart()
                try {
                  val s = f"$sequence%09d".toArray
                  val path =
                    s"${s.slice(0, 3).mkString}/${s.slice(3, 6).mkString}/${s.slice(6, 9).mkString}.osm.gz"

                  logInfo(s"Fetching sequence $sequence")
                  val response = Http(baseUri.resolve(path).toString).asBytes

                  if (response.code === 404) {
                    logInfo(s"$sequence is not yet available, sleeping.")
                    Thread.sleep(15000)
                    restart(s"Retrying sequence $sequence...")
                  } else {
                    val data = XML.loadString(
                      IOUtils.toString(new GZIPInputStream(
                        new ByteArrayInputStream(response.body))))

                    val changesets = (data \ "changeset").map(Changeset.fromXML)

                    logInfo(s"Received ${changesets.length} changesets")

                    store(changesets.iterator)

                    sequence += 1

                    restart(s"Next sequence is $sequence...")
                  }
                } catch {
                  case t: Throwable =>
                    restart("Error receiving data", t)
                }

              }
            }

            val ssc = new StreamingContext(ss.sparkContext, Seconds(30))
            val changesetsStream =
              ssc.receiverStream(new ChangesetsReceiver(None, changesetSource))

            val tags = changesetsStream.map { changeset =>
              (changeset.id, changeset.tags.getOrElse("comment", ""))
            }

            tags.print()

            ssc.start()

            ssc.awaitTermination()

            ss.stop()
        }
      }
    )
