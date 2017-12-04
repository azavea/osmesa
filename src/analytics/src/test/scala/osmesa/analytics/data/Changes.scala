package osmesa.analytics.data

import osmesa.analytics._
import osmesa.analytics.stats._
import TestData._

import org.apache.spark.sql._

import scala.collection.mutable

class Changes() {
  case class Info[T <: OsmElement](
    element: T,
    currentVersion: Long,
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

  private val userStats = mutable.Map[Long, ExpectedUserStats]()
  private val hashtagStats = mutable.Map[Long, ExpectedHashtagStats]()

  private def filterToNewTopics(
    prevTopics: Map[StatTopic, Set[Option[OsmId]]],
    newTopics: Seq[StatTopic]
  ): Map[StatTopic, Set[Option[OsmId]]] = {
    val filteredPrevTopics =
      prevTopics.
        map { case (k, v) => (k, v.flatten.map(Some(_): Option[OsmId])) }.
        filter { case (topic, reasons) =>
          !reasons.isEmpty
        }.
        toMap
    mergeMaps(filteredPrevTopics, newTopics.map((_, Set[Option[OsmId]](None))).toMap)
  }

  def add(changeset: Changeset): Unit = {
    val Changeset(user, hashtags, elements) = changeset

    // Keep track of stat topic (adds/mods) for this changeset
    // by keeping track of the contributing IDs
    var topicCounts: Map[StatTopic, (Set[OsmId], Set[OsmId])] =
      StatTopics.ALL.map(_ -> (Set[OsmId](), Set[OsmId]())).toMap

    // Sort so we encounter nodes first, ways second, and relations last
    val sortedElements =
      elements.sortBy {
        case e: Node => 0
        case e: Way => 1
        case e: Relation =>2
      }

    for(element <- sortedElements) {
      element match {
        case node @ Node(id, lon, lat, tags)  =>
          val topics = StatTopics.tagsToTopics(tags)
          nodeInfo.get(id) match {
            case Some(Info(prevNode, prevVersion, prevTopics)) =>
              // TODO: What happens when the node needs tags from the way of this changeset?
              // Answer: It'll be taken care of in the upstream change. (VERIFY)
              nodeInfo(id) =
                Info(
                  node,
                  prevVersion + 1L,
                  filterToNewTopics(prevTopics, topics)
                )

              topics.
                foreach { topic =>
                  topicCounts.get(topic) match {
                    case Some((adds, mods)) =>
                      topicCounts = topicCounts + (topic -> (adds, mods + NodeId(id)))
                    case None => sys.error(s"Got unexpected topic $topic")
                  }
                }
            case None =>
              // New node
              nodeInfo(id) =
                Info(
                  node,
                  1L,
                  topics.map((_, Set[Option[OsmId]](None))).toMap
                )

              topics.
                foreach { topic =>
                  topicCounts.get(topic) match {
                    case Some((adds, mods)) =>
                      topicCounts = topicCounts + (topic -> (adds + NodeId(id), mods))
                    case None => sys.error(s"Got unexpected topic $topic")
                  }
                }
          }
        case way @ Way(id, nodes, tags) =>
          val topics = StatTopics.tagsToTopics(tags)
          wayInfo.get(id) match {
            case Some(Info(prevWay, prevVersion, prevTopics)) =>
              wayInfo(id) =
                Info(
                  way,
                  prevVersion + 1L,
                  filterToNewTopics(prevTopics, topics)
                )

              topics.
                foreach { topic =>
                  topicCounts.get(topic) match {
                    case Some((adds, mods)) =>
                      topicCounts = topicCounts + (topic -> (adds, mods + WayId(id)))
                    case None => sys.error(s"Got unexpected topic $topic")
                  }
                }
            case None =>
              // New node
              wayInfo(id) =
                Info(
                  way,
                  1L,
                  topics.map((_, Set[Option[OsmId]](None))).toMap
                )

              topics.
                foreach { topic =>
                  topicCounts.get(topic) match {
                    case Some((adds, mods)) =>
                      topicCounts = topicCounts + (topic -> (adds + WayId(id), mods))
                    case None => sys.error(s"Got unexpected topic $topic")
                  }
                }
          }
        case relation @ Relation(id, members, tags) =>
      }
    }
  }

  def historyRows: Seq[Row] = _historyRows.toSeq
  def changesetRows: Seq[Row] = _changesetRows.toSeq
  def stats: (Seq[ExpectedUserStats], Seq[ExpectedHashtagStats]) =
    (userStats.values.toSeq, hashtagStats.values.toSeq)
}
