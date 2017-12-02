package osmesa.analytics.oneoffs

import osmesa.analytics._

import cats.implicits._
import com.monovore.decline._
import com.vividsolutions.jts.geom.Coordinate
import geotrellis.vector.{Feature, Line, Point}
import geotrellis.util.Haversine
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions._
import vectorpipe._

import java.time.Instant
import scala.collection.mutable
import scala.util.{Try, Success, Failure}

case class HashtagCount(tag: String, count: Int)

case class EditorCount(editor: String, count: Int)

case class DayCount(day: Instant, count: Int)

case class UserCount(id: Long, name: String, count: Int)

case class CountryCount(name: String, count: Int)

case class Campaign(
  /** Tag that represents this campaign (lower case, without '#') */
  tag: String,

  /** ZXY template for vector tile set of the campaign's extent. */
  extentUri: String,

  /** Number of ways or relations that are version=1 and linked changeset comments contains hashtag for campaign,
    * and  that have a 'highway' tag, and the value of that tag is one of:
    * "motorway", "trunk", "motorway_link", "trunk_link", "primary", "secondary", "tertiary",
    * "primary_link", "secondary_link", "tertiary_link", "service", "residential", "unclassified",
    * "living_street", or "road".
    */
  roadsAdd: Int,

  /** Number of ways or relations that are version=greater than 1 and linked changeset comments contains hashtag for campaign,
    * OR NODES HAVE CHANGED
    * and  that have a 'highway' tag, and the value of that tag is one of:
    * "motorway", "trunk", "motorway_link", "trunk_link", "primary", "secondary", "tertiary",
    * "primary_link", "secondary_link", "tertiary_link", "service", "residential", "unclassified",
    * "living_street", or "road".
    */
  roadsMod: Int,

  /** Number of ways or relations that are version=1 and linked changeset comments contains hashtag for campaign,
    * that have the tag "building="
    */
  buildingsAdd: Int,

  /** Number of ways or relations that are version=greater than 1 and linked changeset comments
    * contains hashtag for campaign,
    * OR NODES HAVE CHANGED
    * that have the tag "building="
    */
  buildingsMod: Int,

  /** Number of relations or ways that are version=1 and linked changeset comments contains hashtag for campaign,
    * and  that have a 'waterway=' tag
    */
  waterwayAdd: Int,

  /** Number of ways, nodes or relations that are version=1 and linked changeset comments contains hashtag for campaign,
    * and have the tag amenity=
    */
  poiAdd: Int,

  /** For the same criteria as "roadsAdd", the total KM distance between nodes between those ways. */
  kmRoadAdd: Double,

  /** For the same criteria as "roadsMod", the total KM distance between nodes between those ways. */
  kmRoadMod: Double,

  /** For the same criteria as "waterwayAdd", the total KM distance between nodes between those ways. */
  kmWaterwayAdd: Double,

  /** List of participating users */
  users: List[UserCount],

  /** Total number of changesets with this hashtag */
  totalEdits: Long
)

case class User(
  /** UID of the user */
  uid: Long,

  /** Name of the user as per last import */
  name: String,

  /** ZXY template for vector tile set of the users's extent. */
  extent: String,

  buildingCountAdd: Int, // 2

  buildingCountMod: Int,

  poiCountAdd: Int,

  poiCountMod: Int,

  kmWaterwayAdd: Double,

  waterwayCountAdd: Int,

  /** ... */
  kmRoadAdd: Double,

  /** ... */
  kmRoadMod: Double,

  /** ... */
  roadCountAdd: Int,

  /** ... */
  roadCountMod: Int,

  /** Number of changesets that have uid = this user */
  changesetCount: Int,

  /** List of editors that are being used in the changesets. Counted by changeset.
    * Changeset contains a tag that is "created_by=".
    */
  editors: List[EditorCount],

  /** Changeset timestamps, counts by day. */
  editTimes: List[DayCount],

  /** Set of countries that contain nodes that this user has edited in, counted by changeset */
  countries: List[CountryCount],

  /** Set of hashtags this user has contributed to, counted by changeset */
  hashtag: List[HashtagCount]
) {
  /** Merge two user objects to aggregate statistics.
    * Will throw if the IDs are not the same
    */
  def merge(other: User): User =
    if(uid != other.uid) sys.error(s"User IDs do not match, cannot aggregate: ${uid} != ${other.uid}")
    else {
      User(
        uid,
        name,
        extent,
        buildingCountAdd + other.buildingCountAdd,
        buildingCountMod + other.buildingCountMod,
        poiCountAdd + other.poiCountAdd,
        poiCountMod + other.poiCountMod,
        kmWaterwayAdd + other.kmWaterwayAdd,
        waterwayCountAdd + other.waterwayCountAdd,
        kmRoadAdd + other.kmRoadAdd,
        kmRoadMod + other.kmRoadMod,
        roadCountAdd + other.roadCountAdd,
        roadCountMod + other.roadCountMod,
        changesetCount + other.changesetCount,
        editors ++ other.editors,
        editTimes ++ other.editTimes,
        countries ++ other.countries,
        hashtag ++ other.hashtag
      )
    }
}

object User {
  def userExtentUri(userId: Long): String = ???

  def fromChangesetStats(changesetStats: ChangesetStats): User =
    User(
      changesetStats.userId,
      changesetStats.userName,
      userExtentUri(changesetStats.userId),
      changesetStats.buildingsAdded,
      changesetStats.buildingsModified,
      changesetStats.poisAdded,
      changesetStats.poisModified,
      changesetStats.kmWaterwayAdded,
      changesetStats.waterwaysAdded,
      changesetStats.kmRoadAdded,
      changesetStats.kmRoadModified,
      changesetStats.roadsAdded,
      changesetStats.roadsModified,
      1,
      changesetStats.editor.map(EditorCount(_, 1)).toList,
      List(DayCount(changesetStats.closedAt, 1)),
      changesetStats.countries.map(CountryCount(_, 1)),
      changesetStats.hashtags.map(HashtagCount(_, 1))
    )
}

case class ChangesetStats(
  changeset: Long,
  userId: Long,
  userName: String,
  createdAt: Instant,
  closedAt: Instant,
  editor: Option[String],
  hashtags: List[String],
  roadsAdded: Int = 0, // ways or relations added
  roadsModified: Int = 0, // ways, identified by (ways, relations)
  buildingsAdded: Int = 0,// ways or relations added
  buildingsModified: Int = 0, // ways, identified by (ways, relations)
  waterwaysAdded: Int = 0, // ways or relations added
  waterwaysModified: Int = 0, // ways, identified by (ways, relations)
  poisAdded: Int = 0, // nodes, ways or relations added
  poisModified: Int = 0,
  kmRoadAdded: Double = 0.0,
  kmRoadModified: Double = 0.0,
  kmWaterwayAdded: Double = 0.0,
  kmWaterwayModified: Double = 0.0,
  countries: List[String] = List()
)

sealed abstract class OsmId { def id: Long }
case class NodeId(id: Long) extends OsmId
case class WayId(id: Long) extends OsmId
case class RelationId(id: Long) extends OsmId

case class ChangeItem(osmId: OsmId, changeset: Long, isNew: Boolean)

object StatTopics {
  val BUILDING = 'B'
  val ROAD = 'R'
  val WATERWAY = 'W'
  val POI = 'P'
}

class StatCounter private (private val elements: Map[StatTopic, Set[ChangeItem]] = Map()) {
  def +(item: ChangeItem, statTopics: Set[StatTopic]): StatCounter = {
    var e = elements
    for(statTopic <- statTopics) {
      elements.get(statTopic) match {
        case Some(changeItems) => e = e + ((statTopic, changeItems + item))
        case None => e = e + ((statTopic, Set(item)))
      }
    }
    new StatCounter(e)
  }

  def merge(other: StatCounter): StatCounter =
    new StatCounter(
      (elements.toSeq ++ other.elements.toSeq).
        groupBy(_._1).
        map { case (k, vs) =>
          (k, vs.map(_._2).reduce(_ ++ _))
        }.
        toMap
    )

  private def countNew(topic: StatTopic): Int =
    elements.get(topic) match {
      case Some(items) => items.filter(_.isNew).size
      case None => 0
    }

  private def countModified(topic: StatTopic): Int =
    elements.get(topic) match {
      case Some(items) => items.filter(!_.isNew).size
      case None => 0
    }

  def roadsAdded: Int = countNew(StatTopics.ROAD)
  def roadsModified: Int = countModified(StatTopics.ROAD)
  def buildingsAdded: Int = countNew(StatTopics.BUILDING)
  def buildingsModified: Int = countModified(StatTopics.BUILDING)
  def waterwaysAdded: Int = countNew(StatTopics.WATERWAY)
  def waterwaysModified: Int = countModified(StatTopics.WATERWAY)
  def poisAdded: Int = countNew(StatTopics.POI)
  def poisModified: Int = countModified(StatTopics.POI)
}

object StatCounter {
  def apply(id: OsmId, changeset: Long, version: Long, statTopics: Set[StatTopic]): StatCounter =
    new StatCounter() + (ChangeItem(id, changeset, version == 1L), statTopics)

  def apply(): StatCounter =
    new StatCounter()
}

class UpstreamTopics(m: Map[Long, Set[StatTopic]]) {
  private val (changesets, topics) = {
    val sorted = m.toSeq.sortBy(_._1)
    (sorted.map(_._1).toArray, sorted.map(_._2).toArray)
  }

  def forChangeset(changeset: Long): Set[StatTopic] = {
    val i = java.util.Arrays.binarySearch(changesets, changeset)
    if(i >= 0) { topics(i) }
    else {
      // For non matches, binarySearch returns the insertion index as -i - 1
      // We want the index before the insertion point.
      // If the insertion point is 0 (all changesets for the upstream change
      // are later than the changeset parameter), return no Topics.
      val insertionIndex = -i - 1
      if(insertionIndex > 0) { topics(insertionIndex - 1) }
      else { Set() }
    }
  }
}

object ScorecardStats {
  private implicit def changeTopicEncoder: Encoder[Array[StatTopic]] = ExpressionEncoder()

  def statTopics(col: Column): TypedColumn[Any, Array[StatTopic]] =
    udf[Array[StatTopic], Map[String, String]]({ tags =>
      val b = mutable.ArrayBuffer[Char]()
      tags.get("highway") match {
        case Some(v) if Constants.ROAD_VALUES.contains(v) => b += StatTopics.ROAD
        case None => ()
      }

      if(tags contains "building") { b += StatTopics.BUILDING }
      if(tags contains "waterway") { b += StatTopics.WATERWAY }
      if(tags contains "amenity") { b += StatTopics.POI }

      b.toArray
    }).apply(col).as[Array[StatTopic]]

  def computeChangesetStats(history: DataFrame, changesets: DataFrame)(implicit ss: SparkSession): RDD[(Long, ChangesetStats)] = {
    import ss.implicits._

    val changesetPartitioner = new HashPartitioner(10000)
    val wayPartitioner = new HashPartitioner(10000)
    val nodePartitioner = new HashPartitioner(50000)

    def generateUpstreamTopics[T <: OsmId](rdd: RDD[(Long, Iterable[(T, Long, Set[StatTopic])])]): RDD[(Long, List[(T, UpstreamTopics)])] =
      rdd.
        mapValues { idsAndChangesets =>
          (idsAndChangesets.foldLeft(Map[T, Map[Long, Set[StatTopic]]]()) { case (acc, (osmId, changeset, statTopics)) =>
            acc.get(osmId) match {
              case Some(m) => acc + (osmId -> (m + (changeset ->  statTopics)))
              case _ => acc + (osmId -> Map(changeset -> statTopics))
            }
          }).toList.map { case (k, v) => (k, new UpstreamTopics(v)) }
        }


    // Create the base set of ChangesetStats we'll be joining against
    val initialChangesetStats =
      changesets.
        select($"id", $"uid", $"user", $"created_at", hashtags($"tags").alias("hashtags")).
        map { row =>
          val changeset = row.getAs[Long]("id")
          val userId = row.getAs[Long]("uid")
          val userName = row.getAs[String]("user")
          val createdAt = row.getAs[java.sql.Timestamp]("created_at")
          val closedAt = row.getAs[java.sql.Timestamp]("closed_at")
          val tags = row.getAs[Map[String, String]]("tags")
          val editor = tags.get("created_by")

          val hashtags = row.getAs[Array[String]]("hashtags").toList

          (changeset, userId, userName, createdAt, closedAt, editor, hashtags)
        }.
        rdd.
        map { case (changeset, userId, userName, createdAt, closedAt, editor, hashtags) =>
          val stats =
            ChangesetStats(
              changeset,
              userId,
              userName,
              createdAt.toInstant,
              closedAt.toInstant,
              editor,
              hashtags
            )

          (changeset, stats)
        }
        .partitionBy(changesetPartitioner)

    // **  Roll up stats based on changeset ** //

    val relevantRelations =
      history.
        where("type == 'relation'").
        select(
          $"id",
          $"changeset",
          $"version",
          $"members",
          statTopics($"tags").as("statTopics")
        ).
        where(size($"statTopics") > 0)

    val relationStatChanges: RDD[(Long, StatCounter)] =
      relevantRelations.
        select($"id", $"changeset", $"version", $"statTopics").
        rdd.
        map { row =>
          val osmId = RelationId(row.getAs[Long]("id"))
          val changeset = row.getAs[Long]("changeset")
          val version = row.getAs[Long]("version")
          val topics = row.getAs[Array[StatTopic]]("statTopics").toSet
          (changeset, StatCounter(osmId, changeset, version, topics))
        }

    val relationMembers =
      relevantRelations.
        select($"id", $"changeset", $"statTopics", explode($"members").as("member"))

    val relationsForWays: RDD[(Long, List[(RelationId, UpstreamTopics)])] =
      generateUpstreamTopics(
        relationMembers.
          where($"member.type" === "way").
          rdd.
          map { row =>
            val osmId = RelationId(row.getAs[Long]("id"))
            val changeset = row.getAs[Long]("changeset")
            val topics = row.getAs[Array[StatTopic]]("statTopics").toSet
            val wayId = row.getAs[Long]("member.ref")

            (wayId, (osmId, changeset, topics))
          }.
          groupByKey(wayPartitioner)
      )

    val relationsForNodes: RDD[(Long, List[(RelationId, UpstreamTopics)])] =
      generateUpstreamTopics(
        relationMembers.
          where($"member.type" === "node").
          rdd.
          map { row =>
            val osmId = RelationId(row.getAs[Long]("id"))
            val changeset = row.getAs[Long]("changeset")
            val topics = row.getAs[Array[StatTopic]]("statTopics").toSet
            val nodeId = row.getAs[Long]("member.ref")

            (nodeId, (osmId, changeset, topics))
          }.
          groupByKey(nodePartitioner)
      )

    val wayInfo =
      history.
        where("type == 'way'").
        select(
          $"id",
          $"changeset",
          $"version",
          $"nds.ref".as("nodes"),
          statTopics($"tags").as("statTopics")
        ).
        rdd.
        map { row =>
          val id = row.getAs[Long]("id")
          val changeset = row.getAs[Long]("changeset")
          val version = row.getAs[Long]("version")
          val nodeIds = row.getAs[Seq[Long]]("nodes")
          val topics = row.getAs[Array[StatTopic]]("statTopics").toSet
          (id, (changeset, version, nodeIds, topics))
        }

    val wayStatChanges =
      wayInfo.
        leftOuterJoin(relationsForWays).
        filter { case (_, ((_, _, _, topics), relationOpt)) =>
          !topics.isEmpty || relationOpt.isDefined
        }.
        map { case (wayId, ((changeset, version, nodeIds, topics), relationsOpt)) =>
          var statCounter = StatCounter()

          relationsOpt match {
            // List[(RelationId, Map[Long, Set[StatTopic]])]
            case Some(relations) =>
              for((relationId, upstreamTopics) <- relations) {
                val upstreamTopicSet = upstreamTopics.forChangeset(changeset)
                if(!upstreamTopicSet.isEmpty) {
                  statCounter = statCounter + (ChangeItem(relationId, changeset, false), upstreamTopicSet)
                }
              }
            case None => ()
          }

          if(!topics.isEmpty) {
            statCounter = statCounter + (ChangeItem(WayId(wayId), changeset, version == 1L), topics)
          }

          (changeset, statCounter)
        }

    val waysForNodes: RDD[(Long, List[(WayId, UpstreamTopics)])] =
      generateUpstreamTopics(
        wayInfo.
          flatMap { case (wayId, (changeset, _, nodeIds, statTopics)) =>
            nodeIds.map { nodeId =>
              (nodeId, (WayId(wayId), changeset, statTopics))
            }
          }.
          groupByKey(nodePartitioner)
      )

    val nodeInfo =
      history.
        where("type == 'node'").
        select(
          $"id",
          $"changeset",
          $"version",
          $"lat",
          $"lon",
          statTopics($"tags").as("statTopics")
        ).
        rdd.
        map { row =>
          val id = row.getAs[Long]("id")
          val changeset = row.getAs[Long]("changeset")
          val version = row.getAs[Long]("version")
          val lat = row.getAs[Double]("lat")
          val lon = row.getAs[Double]("lon")
          val topics = row.getAs[Array[StatTopic]]("statTopics").toSet
          (id, (changeset, version, new Coordinate(lon, lat), topics))
        }

    val nodeStatChanges =
      nodeInfo.
        cogroup(relationsForNodes, waysForNodes).
        filter { case (_, (nodes, ways, relations)) =>
          nodes.foldLeft(false) { case (acc, (_, _, _, statTopics)) => acc || !statTopics.isEmpty } ||
          !ways.isEmpty ||
          !relations.isEmpty
        }.
        flatMap { case (nodeId, (nodes, ways, relations)) =>
          for((changeset, version, coord, topics) <- nodes) yield {
            var statCounter = StatCounter()

            for((relationId, upstreamTopics) <- relations.flatten) {
              val upstreamTopicSet = upstreamTopics.forChangeset(changeset)
              if(!upstreamTopicSet.isEmpty) {
                statCounter = statCounter + (ChangeItem(relationId, changeset, false), upstreamTopicSet)
              }
            }

            if(!topics.isEmpty) {
              statCounter = statCounter + (ChangeItem(NodeId(nodeId), changeset, version == 1L), topics)
            }

            (changeset, statCounter)
          }
        }

    val mergedStatChanges =
      ss.sparkContext.union(relationStatChanges, wayStatChanges, nodeStatChanges).
        reduceByKey(changesetPartitioner, _ merge _)

    initialChangesetStats.
      leftOuterJoin(mergedStatChanges).
      mapValues { case (stats, counterOpt) =>
        counterOpt match {
          case Some(counter) =>
            stats.copy(
              roadsAdded = counter.roadsAdded,
              roadsModified = counter.roadsModified,
              buildingsAdded = counter.buildingsAdded,
              buildingsModified = counter.buildingsModified,
              waterwaysAdded = counter.waterwaysAdded,
              waterwaysModified = counter.waterwaysModified,
              poisAdded = counter.poisAdded,
              poisModified = counter.poisModified// ,
              // kmRoadAdded = 0.0,
              // kmRoadModified = 0.0,
              // kmWaterwayAdded = 0.0,
              // kmWaterwayModified = 0.0,
              // countries = List()
            )
          case None => stats
        }
      }
  }

  def computeUserStats(changesetStats: RDD[(Long, ChangesetStats)]): RDD[User] =
    changesetStats
      .map { case (_, changesetStat) =>
        (changesetStat.userId, User.fromChangesetStats(changesetStat))
      }
      .reduceByKey(_ merge _ )
      .map(_._2)

  def computeCampaignStats(changesetStats: RDD[(Long, ChangesetStats)]): RDD[Campaign] = ???

  def compute(history: DataFrame, changesets: DataFrame)(implicit ss: SparkSession): (RDD[Campaign], RDD[User]) = {
    val changesetStats = computeChangesetStats(history, changesets)

    (computeCampaignStats(changesetStats), computeUserStats(changesetStats))
  }

  // /* The highway (road) types. */
  // val highways: Set[String] = Set(
  //   "motorway", "trunk", "motorway_link", "trunk_link", "primary", "secondary", "tertiary",
  //   "primary_link", "secondary_link", "tertiary_link", "service", "residential", "unclassified",
  //   "living_street", "road"
  // )

  // /** How long is a Line, in metres? */
  // private[this] def metres(line: Line): Double = {
  //   val ps: List[Point] = line.points.toList
  //   val pairs: Iterator[(Point, Point)] = ps.iterator.zip(ps.tail.iterator)

  //   pairs.foldLeft(0d) { case (acc, (p,c)) => acc + Haversine(p.x, p.y, c.x, c.y) }
  // }

  // /* Lengths of new roads created by each user */
  // def newRoadsByUser(data: DataFrame)(implicit ss: SparkSession): Try[RDD[(Long, Double)]] = {

  //   Try(osm.fromDataFrame(data)).map { case (nodes, ways, relations) =>
  //     val roadsOnly: RDD[(Long, osm.Way)] =
  //       ways.filter(_._2.meta.tags.get("highway").map(highways.contains(_)).getOrElse(false))

  //     /* Roads that only had one version, implying that they were new */
  //     val news: RDD[(Long, osm.Way)] = roadsOnly.groupByKey
  //       .filter { case (_, iter) => iter.size === 1 }
  //       .map { case (l, ws) => (l, ws.head) }

  //     val lines: RDD[Feature[Line, osm.ElementMeta]] = osm.toHistory(nodes, news)._2

  //     /* The `Long` is now the user's unchanging unique ID */
  //     val byUsers: RDD[(Long, Iterable[Feature[Line, osm.ElementMeta]])] =
  //       lines.map(f => (f.data.uid, f)).groupByKey

  //     byUsers.mapValues(fs => fs.map(f => metres(f.geom)).foldLeft(0d)(_ + _))
  //   }
  // }

}

// object ScorecardStatsCommand extends CommandApp(

//   name   = "scorecard-stats",
//   header = "Compute scorecard stats.",
//   main   = {

//     val orcO = Opts.option[String]("orc", help = "Location of the ORC file to process.")

//     orcO.map { orc =>

//       /* Settings compatible for both local and EMR execution */
//       val conf = new SparkConf()
//         .setIfMissing("spark.master", "local[*]")
//         .setAppName("road-changes")
//         .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//         .set("spark.kryo.registrator", classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)
//         .set("spark.sql.orc.filterPushdown", "true")

//       implicit val ss: SparkSession = SparkSession.builder
//         .config(conf)
//         .enableHiveSupport
//         .getOrCreate

//       // Logger.getRootLogger().setLevel(Level.INFO)

//       try {
//         ScorecardStats.compute(ss.read.orc(orc))
//       } finally {
//         ss.stop()
//       }
//     }
//   }
// )
