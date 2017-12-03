package osmesa.analytics.data

import osmesa.analytics._
import osmesa.analytics.stats._

import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json._
import org.apache.spark.sql._

import java.sql.Timestamp
import scala.collection.mutable

object TestData {
  case class User(uid: Long, name: String)
  object User {
    val Bob = User(1L, "Bob")
    val Alice = User(2L, "Alice")
    val Carson = User(3L, "Carson")
    val Reese = User(4L, "Reese")
    val Alshon = User(5L, "Alshon")
  }

  case class Hashtag(name: String)
  object Hashtag {
    val Mapathon = Hashtag("Mapathon")
    val OnlyPeru = Hashtag("OnlyPeru")
    val GoEagles = Hashtag("GoEagles")
  }

  abstract sealed trait OsmChange
  case class NodeChange(node: Node) extends OsmChange
  case class WayChange(way: Way) extends OsmChange
  case class RelationChange(relation: Relation) extends OsmChange

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
      val result = Node(createNodeId, x, y, tags)
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
    changes: Seq[OsmChange]
  )

  class Changes() {
    case class Info[T <: OsmElement](
      element: T,
      currentVersion: Long,
      hashtags: Map[String, Set[Option[OsmId]]], // Mapped to the contributing elements, or None if self.
      topics: Map[StatTopic, Set[Option[OsmId]]] // Mapped to the contributing elements, or None if self.
    )

    private val _historyRows = mutable.ListBuffer[Row]()
    private val _changesetRows = mutable.ListBuffer[Row]()

    private val nodeInfo = mutable.Map[Long, Info[Node]]()
    private val wayInfo = mutable.Map[Long, Info[Way]]()
    private val relationInfo = mutable.Map[Long, Info[Relation]]()

    private val nodesToWays = mutable.Map[Long, Long]()
    private val nodesToRelations = mutable.Map[Long, Long]()
    private val waysToRelations = mutable.Map[Long, Long]()

    private val userStats = mutable.Map[Long, UserStats]()
    private val hashtagStats = mutable.Map[Long, HashtagStats]()

    def add(changeset: Changeset): Unit = {
      ???
    }

    def historyRows: Seq[Row] = _historyRows.toSeq
    def changesetRows: Seq[Row] = _changesetRows.toSeq
    def stats: (Seq[UserStats], Seq[HashtagStats]) =
      (userStats.values.toSeq, hashtagStats.values.toSeq)
  }

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
