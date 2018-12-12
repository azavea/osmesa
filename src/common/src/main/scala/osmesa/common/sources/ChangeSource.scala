package osmesa.common.sources

import java.io.{ByteArrayInputStream, IOException, StringReader}
import java.net.URI
import java.util.Properties
import java.util.zip.GZIPInputStream

import com.softwaremill.macmemo.memoize
import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging
import org.joda.time.DateTime
import osmesa.common.model.{Actions, Change}
import scalaj.http.Http

import scala.concurrent.duration.{Duration, _}
import scala.xml.XML

object ChangeSource extends Logging {
  val Delay: Duration = 15 seconds

  def getSequence(baseURI: URI, sequence: Int): Seq[Change] = {
    val s = f"$sequence%09d".toArray
    val path =
      s"${s.slice(0, 3).mkString}/${s.slice(3, 6).mkString}/${s.slice(6, 9).mkString}.osc.gz"

    logInfo(s"Fetching sequence $sequence")

    try {
      val response =
        Http(baseURI.resolve(path).toString).asBytes

      if (response.code == 404) {
        logInfo(s"$sequence is not yet available, sleeping.")
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
            (node \ "_").map(Change.fromXML(_, action, sequence))
          }

          logDebug(s"Received ${changes.length} changes")

          changes
        } finally {
          gzis.close()
          bais.close()
        }
      }
    } catch {
      case e: IOException =>
        logWarning(s"Error fetching change $sequence", e)
        Thread.sleep(Delay.toMillis)
        getSequence(baseURI, sequence)
    }
  }

  @memoize(maxSize = 1, expiresAfter = 30 seconds)
  def getCurrentSequence(baseURI: URI): Option[Int] = {
    try {
      val response =
        Http(baseURI.resolve("state.txt").toString).asString

      val state = new Properties
      state.load(new StringReader(response.body))

      val sequence = state.getProperty("sequenceNumber").toInt
      val timestamp = DateTime.parse(state.getProperty("timestamp"))

      logDebug(s"$baseURI state: $sequence @ $timestamp")

      Some(sequence)
    } catch {
      case err: Throwable =>
        logError("Error fetching or parsing changeset state.", err)
        logError(baseURI.toString)

        None
    }
  }
}
