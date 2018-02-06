package osmesa.analytics.data

import osmesa.analytics._
import osmesa.analytics.stats._

import com.vividsolutions.jts.geom.Coordinate
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json._
import org.apache.spark.sql._

import java.sql.Timestamp
import java.math.BigDecimal
import java.math.RoundingMode
import scala.collection.mutable

object TestData {
  case class User(id: Long, name: String)
  object User {
    val Bob = User(1L, "Bob")
    val Alice = User(2L, "Alice")
    val Carson = User(3L, "Carson")
    val Reese = User(4L, "Reese")
    val Alshon = User(5L, "Alshon")
  }

  case class Hashtag(name: String)
  object Hashtag {
    // TODO: Right now these need to be lower case becaues of the toLowerCase call
    // happening in the stats calculation.
    val Mapathon = Hashtag("mapathon")
    val OnlyPeru = Hashtag("onlyperu")
    val GoEagles = Hashtag("goeagles")
  }

  class Elements() {
    private var newNodeId = 11100L
    private var newWayId = 1100L
    private var newRelationId = 100L

    private val nodesByName = mutable.Map[String, Node]()
    private val waysByName = mutable.Map[String, Way]()
    private val relationsByName = mutable.Map[String, Relation]()

    def createNodeId: Long = {
      val v = newNodeId
      newNodeId += 1
      v
    }

    def createWayId: Long = {
      val v = newWayId
      newWayId += 1
      v
    }

    def createRelationId: Long = {
      val v = newRelationId
      newRelationId += 1
      v
    }

    def newNode(x: Double, y: Double, tags: Map[String, String], name: Option[String]): Node = {
      // Create the appropriate resolution decimal.
      val lon = new BigDecimal(x).setScale(7, RoundingMode.HALF_UP).doubleValue
      val lat = new BigDecimal(y).setScale(7, RoundingMode.HALF_UP).doubleValue
      val result = Node(createNodeId, lon, lat, tags)
      name match {
        case Some(n) => nodesByName(n) = result ; result
        case None => result
      }
    }

    def newNode(p: Point, tags: Map[String, String], name: Option[String]): Node =
      newNode(p.x, p.y, tags, name)

    def newWay(nodes: Seq[Node], tags: Map[String, String], name: Option[String]): Way = {
      val result =
        Way(createWayId, nodes, tags)

      name match {
        case Some(n) => waysByName(n) = result ; result
        case None => result
      }
    }

    def newWay(wayLine: Line, tags: Map[String, String], name: Option[String]): Way = {
      newWay(wayLine.vertices.map { p => newNode(p, Map(), None) }, tags, name)
    }

    def newRelation(elements: Seq[OsmElement], tags: Map[String, String], name: Option[String]): Relation = {
      val result =
        Relation(createRelationId, elements, tags)

      name match {
        case Some(n) => relationsByName(n) = result ; result
        case None => result
      }
    }

    def nodeByName(name: String): Node = nodesByName(name)
    def wayByName(name: String): Way = waysByName(name)
    def relationByName(name: String): Relation = relationsByName(name)
  }

  abstract sealed trait OsmElement { def id: Long }
  case class Node(id: Long, lon: Double, lat: Double, tags: Map[String, String]) extends OsmElement
  case class Way(id: Long, nodes: Seq[Node], tags: Map[String, String]) extends OsmElement
  case class Relation(id: Long, elements: Seq[OsmElement], tags: Map[String, String]) extends OsmElement

  case class Changeset(
    user: User,
    hashtags: Seq[Hashtag],
    changes: Seq[OsmElement]
  )

  def createHistory(rows: Seq[Row])(implicit ss: SparkSession): DataFrame =
    ss.createDataFrame(
      ss.sparkContext.parallelize(rows),
      Schemas.history
    )

  def createChangesets(rows: Seq[Row])(implicit ss: SparkSession): DataFrame =
    ss.createDataFrame(
      ss.sparkContext.parallelize(rows),
      Schemas.changesets
    )
}
