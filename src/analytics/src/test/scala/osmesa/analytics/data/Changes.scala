package osmesa.analytics.data

import osmesa.analytics._
import osmesa.analytics.stats._
import TestData._

import com.vividsolutions.jts.geom.Coordinate
import org.apache.spark.sql._

import java.time._
import java.time.temporal._
import java.sql.Timestamp
import java.math.BigDecimal
import scala.collection.mutable

class Changes() {
  case class Info[T <: OsmElement](
    element: T,
    currentVersion: Long,
    topics: Map[StatTopic, Set[Option[OsmId]]] // Mapped to the contributing elements, or None if self.
  )

  private val countryLookup = new CountryLookup

  private val _historyRows = mutable.Map[OsmId, List[Row]]()
  private val _changesetRows = mutable.ListBuffer[Row]()
  private var changesetId: Long = 0L

  private val nodeInfo = mutable.Map[Long, Info[Node]]()
  private val wayInfo = mutable.Map[Long, Info[Way]]()
  private val relationInfo = mutable.Map[Long, Info[Relation]]()

  private val nodesToWays = mutable.Map[Long, Long]()
  private val nodesToRelations = mutable.Map[Long, Long]()
  private val waysToRelations = mutable.Map[Long, Long]()

  private val userStats = mutable.Map[Long, ExpectedUserStats]()
  private val hashtagStats = mutable.Map[String, ExpectedHashtagStats]()

  private def getTimestamp(changeset: Long): Timestamp = {
    val dt = ZonedDateTime.of(2015, 5, 17, 12, 0, 0, 0, ZoneOffset.UTC)
    new Timestamp(dt.plus(5, ChronoUnit.SECONDS).toInstant().toEpochMilli)
  }

  private def addHistoryRow[T <: OsmElement](user: User, info: Info[T]): Unit = {
    info.element match {
      case Node(id, lon, lat, tags) =>
        _historyRows(NodeId(id)) = {
          val prev: List[Row] =
            _historyRows.get(NodeId(id)) match {
              case Some(s) =>
                // Ensure all other rows are inivisible
                s.map { row => Row((false :: row.toSeq.toList.reverse.tail).reverse:_*) }
              case None => List()
            }
          Row(
            id, "node", tags, new BigDecimal(lat), new BigDecimal(lon),
            null, null,
            changesetId, getTimestamp(changesetId), user.id, user.name, info.currentVersion, true
          ) :: prev
        }
      case Way(id, nodes, tags) =>
        _historyRows(WayId(id)) = {
          val prev: List[Row] =
            _historyRows.get(WayId(id)) match {
              case Some(s) =>
                // Ensure all other rows are inivisible
                s.map { row => Row((false :: row.toSeq.toList.reverse.tail).reverse:_*) }
              case None => List()
            }
          Row(
            id, "way", tags, null, null,
            nodes.map { n => Row(n.id) }.toArray, null,
            changesetId, getTimestamp(changesetId), user.id, user.name, info.currentVersion, true
          ) :: prev
        }
      case Relation(id, elements, tags) =>
        _historyRows(RelationId(id)) = {
          val prev: List[Row] =
            _historyRows.get(RelationId(id)) match {
              case Some(s) =>
                // Ensure all other rows are inivisible
                s.map { row => Row((false :: row.toSeq.toList.reverse.tail).reverse:_*) }
              case None => List()
            }
          Row(
            id, "way", tags, null, null,
            null,
            elements.map {
              case Node(id, _, _, _) => Row("node", id, null)
              case Way(id, _, _) => Row("way", id, null)
              case Relation(id, _, _) => Row("relation", id, null)
            }.toArray,
            changesetId, getTimestamp(changesetId), user.id, user.name, info.currentVersion, true
          ) :: prev
        }
    }
  }

  private def addChangesetRow(changeset: Changeset): Unit = {
    val Changeset(user, hashtags, elements) = changeset
    _changesetRows += {
      Row(
        changesetId, Map("comment" -> hashtags.map { h => s"#${h.name}" }.mkString(",")),
        getTimestamp(changesetId), false, getTimestamp(changesetId),
        0L, 0.0, 0.0, 0.0, 0.0, elements.size.toLong, // These are not real
        user.id, user.name
      )
    }

    changesetId += 1L
  }

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
    mergeSetMaps(filteredPrevTopics, newTopics.map((_, Set[Option[OsmId]](None))).toMap)
  }

  private def updateUserStat(user: User)(f: ExpectedUserStats => ExpectedUserStats): Unit = {
    userStats.get(user.id) match {
      case Some(stats) =>
        userStats(user.id) = f(stats)
      case None =>
        userStats(user.id) = f(ExpectedUserStats(user))
    }
  }

  private def updateHashtagStat(hashtag: Hashtag)(f: ExpectedHashtagStats => ExpectedHashtagStats): Unit = {
    hashtagStats.get(hashtag.name) match {
      case Some(stats) =>
        hashtagStats(hashtag.name) = f(stats)
      case None =>
        hashtagStats(hashtag.name) = f(ExpectedHashtagStats(hashtag))
    }
  }

  // Adds a node, returns if it was new or not.
  private def addNode(user: User, node: Node, topics: Set[StatTopic], cause: Option[OsmId]): (Boolean, Map[StatTopic, Set[Option[OsmId]]]) = {
    countryLookup.lookup(new Coordinate(node.lon, node.lat)) match {
      case Some(c) =>
        updateUserStat(user) { stat =>
          stat.copy(countries = stat.countries + c.name)
        }
      case None => ()
    }

    val (info, isNew, topicMap) =
      nodeInfo.get(node.id) match {
        case Some(Info(prevNode, prevVersion, prevTopics)) =>
          val filteredPrevTopics =
            prevTopics.
              map { case (k, v) => k -> (v - cause) }.
              toMap

          val topicMap =
            mergeSetMaps(filteredPrevTopics, topics.map((_, Set(cause))).toMap)

          (Info(
            node,
            prevVersion + 1L,
            topicMap
          ), false, topicMap)
        case None =>
          val topicMap =
            topics.map((_, Set(cause))).toMap

          (Info(
            node,
            1L,
            topicMap
          ), true, topicMap)
      }
    addHistoryRow(user, info)
    nodeInfo(node.id) = info
    (isNew, topicMap)
  }

  // Adds a way, returns if it was new or not.
  private def addWay(user: User, way: Way, topics: Set[StatTopic], cause: Option[OsmId]): (Boolean, Map[StatTopic, Set[Option[OsmId]]]) = {
    val (info, isNew, topicMap) =
      wayInfo.get(way.id) match {
        case Some(Info(prevway, prevVersion, prevTopics)) =>
          val filteredPrevTopics =
            prevTopics.
              map { case (k, v) => k -> (v - cause) }.
              toMap

          val topicMap =
            mergeSetMaps(filteredPrevTopics, topics.map((_, Set(cause))).toMap)

          (Info(
            way,
            prevVersion + 1L,
            topicMap
          ), false, topicMap)
        case None =>
          val topicMap =
            topics.map((_, Set(cause))).toMap

          (Info(
            way,
            1L,
            topicMap
          ), true, topicMap)
      }
    addHistoryRow(user, info)
    wayInfo(way.id) = info
    (isNew, topicMap)
  }

  // Adds a relation, returns if it was new or not.
  private def addRelation(user: User, relation: Relation, topics: Set[StatTopic], cause: Option[OsmId]): (Boolean, Map[StatTopic, Set[Option[OsmId]]]) = {
    val (info, isNew, topicMap) =
      relationInfo.get(relation.id) match {
        case Some(Info(prevRelation, prevVersion, prevTopics)) =>
          val filteredPrevTopics =
            prevTopics.
              map { case (k, v) => k -> (v - cause) }.
              toMap

          val topicMap =
            mergeSetMaps(filteredPrevTopics, topics.map((_, Set(cause))).toMap)

          (Info(
            relation,
            prevVersion + 1L,
            topicMap
          ), false, topicMap)
        case None =>
          val topicMap =
            topics.map((_, Set(cause))).toMap
          (Info(
            relation,
            1L,
            topicMap
          ), true, topicMap)
      }
    addHistoryRow(user, info)
    relationInfo(relation.id) = info
    (isNew, topicMap)
  }

  /** Adds a changeset from a test case to this set of changes.
    * Nodes that are part of ways should be specified as part
    * of the ways, and not outside of it; nodes and ways that are part of
    * ways should be specified as part of the relation. For example, if adding
    * a new way with two nodes, one should specify a single element which is the way, and
    * not 3 elements adding the nodes separate from the way.
    */
  def add(changeset: Changeset): Unit = {
    val Changeset(user, hashtags, elements) = changeset

    for(element <- elements) {
      element match {
        case node @ Node(id, lon, lat, tags)  =>
          val topics = StatTopics.tagsToTopics(tags).toSet

          val (isNew, fullTopics) = addNode(user, node, topics, None)

          fullTopics.
            foreach { case (topic, _) =>
              updateUserStat(user)(_.updateTopic(topic, isNew))
              hashtags.foreach { tag =>
                updateHashtagStat(tag)(_.updateTopic(topic, isNew))
              }
            }

        case way @ Way(id, nodes, tags) =>
          val topics = StatTopics.tagsToTopics(tags).toSet

          for(node <- nodes) {
            val (isNew, nodeTopics) = addNode(user, node, topics, Some(WayId(id)))
            nodeTopics.
              foreach { case (topic, cause) =>
                cause.toList match {
                  case Some(osmId) :: Nil if osmId == WayId(id) => ()
                  case _ =>
                    updateUserStat(user)(_.updateTopic(topic, isNew))
                    hashtags.foreach { tag =>
                      updateHashtagStat(tag)(_.updateTopic(topic, isNew))
                    }
                }
              }
          }

          val (isNew, fullTopics) = addWay(user, way, topics, None)

          fullTopics.
            foreach { case (topic, _) =>
              updateUserStat(user)(_.updateTopic(topic, isNew))
              hashtags.foreach { tag =>
                updateHashtagStat(tag)(_.updateTopic(topic, isNew))
              }
            }

        case relation @ Relation(id, members, tags) =>
          val topics = StatTopics.tagsToTopics(tags).toSet

          for(member <- members) {
            member match {
              case node: Node =>
                val (isNew, nodeTopics) = addNode(user, node, topics, Some(RelationId(id)))
                nodeTopics.
                  foreach { case (topic, cause) =>
                    cause.toList match {
                      case Some(osmId) :: Nil if osmId == RelationId(id) => ()
                      case _ =>
                        updateUserStat(user)(_.updateTopic(topic, isNew))
                        hashtags.foreach { tag =>
                          updateHashtagStat(tag)(_.updateTopic(topic, isNew))
                        }
                    }
                  }

              case way: Way =>
                val (isNew, wayTopics) = addWay(user, way, topics, Some(RelationId(id)))
                wayTopics.
                  foreach { case (topic, cause) =>
                    cause.toList match {
                      case Some(osmId) :: Nil if osmId == RelationId(id) => ()
                      case _ =>
                        updateUserStat(user)(_.updateTopic(topic, isNew))
                        hashtags.foreach { tag =>
                          updateHashtagStat(tag)(_.updateTopic(topic, isNew))
                        }
                    }
                  }
              case relation: Relation =>
                sys.error("We do not yet support relations of relations.")
            }
          }

          val (isNew, fullTopics) = addRelation(user, relation, topics, None)

          fullTopics.
            foreach { case (topic, _) =>
              updateUserStat(user)(_.updateTopic(topic, isNew))
              hashtags.foreach { tag =>
                updateHashtagStat(tag)(_.updateTopic(topic, isNew))
              }
            }
      }
    }

    updateUserStat(user) { stat => stat.copy(hashtags = stat.hashtags ++ hashtags) }
    hashtags.foreach { tag =>
      updateHashtagStat(tag) { stat =>
        stat.copy(
          users = stat.users + user,
          totalEdits = stat.totalEdits + 1
        )
      }
    }

    addChangesetRow(changeset)
  }

  def historyRows: Seq[Row] = _historyRows.flatMap(_._2).toSeq
  def changesetRows: Seq[Row] = _changesetRows.toSeq
  def stats: (Seq[ExpectedUserStats], Seq[ExpectedHashtagStats]) =
    (userStats.values.toSeq, hashtagStats.values.toSeq)
}
