package osmesa

import java.util.concurrent.TimeUnit

import org.apache.commons.io.IOUtils
import org.openjdk.jmh.annotations._
import vectorpipe.model.{Actions, Change}

import java.util.zip.GZIPInputStream
import javax.xml.parsers.{SAXParser, SAXParserFactory}
import scala.xml.XML

// --- //

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
class SAXBench {

  val sequence = 0

  @Setup
  def setup: Unit = {
  }

  def gzipInputStream(): GZIPInputStream = {
    // requires the addition of a gzipped OSC file in bench/src/main/resources
    val stream = getClass.getResourceAsStream("/942.osc.gz")
    new GZIPInputStream(stream)
  }

  def withScalaXML(): Int = {
    // requires Change.fromXML (see commit 1b04a1e81f1a88f374a086c98d58677ec537b1bf)
    val data = XML.loadString(IOUtils.toString(gzipInputStream))

    val changes = (data \ "_").flatMap { node =>
      (node \ "_").map(Change.fromXML(_, Actions.fromString(node.label), sequence))
    }

    changes.length
  }

  def withSAXParser(): Int = {
    val factory = SAXParserFactory.newInstance
    val parser = factory.newSAXParser
    val handler = new Change.ChangeHandler(sequence)
    parser.parse(gzipInputStream(), handler)
    handler.changeSeq.length
  }

  @Benchmark
  def useScala: Double = withScalaXML()
  @Benchmark
  def getSAXyGirl: Double = withSAXParser()

}
