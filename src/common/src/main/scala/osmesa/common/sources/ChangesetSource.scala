package osmesa.common.sources

import java.io.{ByteArrayInputStream, IOException}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPInputStream

import cats.implicits._
import com.softwaremill.macmemo.memoize
import io.circe.generic.auto._
import io.circe.{yaml, _}
import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import osmesa.common.model.Changeset
import scalaj.http.Http

import scala.concurrent.duration.{Duration, _}
import scala.xml.XML

object ChangesetSource extends Logging {
  val Delay: Duration = 15 seconds
  // state.yaml uses a custom date format
  private val formatter = DateTimeFormat.forPattern("y-M-d H:m:s.SSSSSSSSS Z")

  private implicit val dateTimeDecoder: Decoder[DateTime] =
    Decoder.instance(a => a.as[String].map(DateTime.parse(_, formatter)))

  def getSequence(baseURI: URI, sequence: Int): Seq[Changeset] = {
    val s = f"$sequence%09d"
    val path = s"${s.slice(0, 3)}/${s.slice(3, 6)}/${s.slice(6, 9)}.osm.gz"

    logDebug(s"Fetching sequence $sequence")

    try {
      val response =
        Http(baseURI.resolve(path).toString).asBytes

      if (response.code == 404) {
        logDebug(s"$sequence is not yet available, sleeping.")
        Thread.sleep(Delay.toMillis)
        getSequence(baseURI, sequence)
      } else {
        // NOTE: if diff bodies get really large, switch to a SAX parser to help with the memory footprint
        val bais = new ByteArrayInputStream(response.body)
        val gzis = new GZIPInputStream(bais)
        try {
          val data = XML.loadString(IOUtils.toString(gzis, StandardCharsets.UTF_8))

          val changesets = (data \ "changeset").map(Changeset.fromXML(_, sequence))

          logDebug(s"Received ${changesets.length} changesets")

          changesets
        } finally {
          gzis.close()
          bais.close()
        }
      }
    } catch {
      case e: IOException =>
        logWarning(s"Error fetching changeset $sequence", e)
        Thread.sleep(Delay.toMillis)
        getSequence(baseURI, sequence)
    }
  }

  @memoize(maxSize = 1, expiresAfter = 30 seconds)
  def getCurrentSequence(baseURI: URI): Option[Int] = {
    try {
      val response =
        Http(baseURI.resolve("state.yaml").toString).asString

      val state = yaml.parser
        .parse(response.body)
        .leftMap(err => err: Error)
        .flatMap(_.as[State])
        .valueOr(throw _)

      logDebug(s"$baseURI state: ${state.sequence} @ ${state.last_run}")

      Some(state.sequence)
    } catch {
      case err: Throwable =>
        logError("Error fetching / parsing changeset state.", err)

        None
    }
  }

  case class State(last_run: DateTime, sequence: Int)
}
