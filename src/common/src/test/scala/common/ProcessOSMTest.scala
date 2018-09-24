package osmesa.common

import geotrellis.vector._
import geotrellis.geotools._
// import geotrellis.geomesa.geotools. _
import org.locationtech.geomesa.spark.jts._
import org.scalatest._
import org.geotools.feature.simple.SimpleFeatureImpl
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.util.Try

class ProcessOSMTest extends FunSpec with TestEnvironment with Matchers {
  import ss.implicits._
  ss.withJTS

  val orcFile = "file:/Users/eugene/Downloads/isle-of-man-latest.osm.orc"

  val elements = ss.read.orc(orcFile)
  val nodes = ProcessOSM.preprocessNodes(elements).cache
  val nodeGeoms = ProcessOSM.constructPointGeometries(nodes).cache
  val wayGeoms = ProcessOSM.reconstructWayGeometries(elements, nodes).cache
  val relationGeoms = ProcessOSM.reconstructRelationGeometries(elements, wayGeoms).cache

  it("parses isle of man nodes") {
    info(s"Nodes: ${nodeGeoms.count}")
  }

  it("parses isle of man wasys") {
    info(s"Ways: ${wayGeoms.count}")
  }

  ignore("parses isle of man relations") {
    info(s"Relations: ${relationGeoms.count}")
  }  
}
