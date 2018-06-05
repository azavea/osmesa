package osmesa.common.streaming

import java.io.{ByteArrayInputStream, IOException, StringReader}
import java.net.URI
import java.util.Properties
import java.util.zip.GZIPInputStream

import cats.implicits._
import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging
import org.joda.time.DateTime
import osmesa.common.model.{Actions, Element}
import scalaj.http.Http

import scala.concurrent.duration.{Duration, _}
import scala.xml.XML

object ChangesSource extends Logging {
  val Delay: Duration = 15.seconds

  def getCurrentSequence(baseURI: URI): Int = {
    val response =
      Http(baseURI.resolve("state.txt").toString).asString

    val state = new Properties
    state.load(new StringReader(response.body))

    val sequence = state.getProperty("sequenceNumber").toInt
    val timestamp = DateTime.parse(state.getProperty("timestamp"))

    logDebug(s"$baseURI state: $sequence @ $timestamp")

    sequence
  }

  private[streaming] def createOffsetForCurrentSequence(baseURI: URI): SequenceOffset =
    SequenceOffset(getCurrentSequence(baseURI))

  def getSequence(baseURI: URI, sequence: Long): Seq[Element] = {
    val s = f"$sequence%09d".toArray
    val path =
      s"${s.slice(0, 3).mkString}/${s.slice(3, 6).mkString}/${s.slice(6, 9).mkString}.osc.gz"

    logDebug(s"Fetching sequence $sequence")
    val response = Http(baseURI.resolve(path).toString).asBytes

    if (response.code === 404) {
      logDebug(s"$sequence is not yet available, sleeping.")
      Thread.sleep(Delay.toMillis)
      getSequence(baseURI, sequence)
    } else {
      // NOTE: if diff bodies get really large, switch to a SAX parser to help with the memory footprint
      val bais = new ByteArrayInputStream(response.body)
      val gzis = new GZIPInputStream(bais)
      try {
        val data = XML.loadString(IOUtils.toString(gzis))

        val changes = (data \ "_").flatMap { node =>
          val action = node.label match {
            case "create" => Actions.Create
            case "modify" => Actions.Modify
            case "delete" => Actions.Delete
          }
          (node \ "_").map(Element.fromXML(_, action))
        }

        logDebug(s"Received ${changes.length} changes")

        changes
      } catch {
        case e: IOException =>
          logWarning(s"Error reading change $sequence", e)
          Thread.sleep(Delay.toMillis)
          getSequence(baseURI, sequence)
      } finally {
        gzis.close()
        bais.close()
      }
    }
  }
}
