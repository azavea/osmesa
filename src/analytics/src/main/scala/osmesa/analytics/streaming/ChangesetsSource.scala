package osmesa.analytics.streaming

import java.io.ByteArrayInputStream
import java.net.URI
import java.util.zip.GZIPInputStream

import cats.implicits._
import io.circe.generic.auto._
import io.circe.{yaml, _}
import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scalaj.http.Http

import scala.annotation.tailrec
import scala.xml.XML

object ChangesetsSource extends Logging {
  // state.yaml uses a custom date format
  private val formatter = DateTimeFormat.forPattern("y-M-d H:m:s.SSSSSSSSS Z")

  private implicit val dateTimeDecoder: Decoder[DateTime] =
    Decoder.instance(a => a.as[String].map(DateTime.parse(_, formatter)))

  case class ChangesetsState(last_run: DateTime, sequence: Int)

  private[streaming] def createInitialOffset(baseURI: URI): SequenceOffset = {
    // TODO extract this
    val response =
      Http(baseURI.resolve("state.yaml").toString).asString

    val state = yaml.parser
      .parse(response.body)
      .leftMap(err => err: Error)
      .flatMap(_.as[ChangesetsState])
      .valueOr(throw _)

    logInfo(s"$baseURI state: ${state.sequence} @ ${state.last_run}")

    SequenceOffset(state.sequence)
  }

  @tailrec
  def getSequence(baseURI: URI, sequence: Long): Seq[Changeset] = {
    val s = f"$sequence%09d".toArray
    val path =
      s"${s.slice(0, 3).mkString}/${s.slice(3, 6).mkString}/${s.slice(6, 9).mkString}.osm.gz"

    logInfo(s"Fetching sequence $sequence")
    val response = Http(baseURI.resolve(path).toString).asBytes

    if (response.code === 404) {
      logInfo(s"$sequence is not yet available, sleeping.")
      Thread.sleep(15000)
      getSequence(baseURI, sequence)
    } else {
      val data = XML.loadString(
        IOUtils.toString(new GZIPInputStream(
          new ByteArrayInputStream(response.body))))

      val changesets = (data \ "changeset").map(Changeset.fromXML)

      logInfo(s"Received ${changesets.length} changesets")

      changesets
    }
  }
}
