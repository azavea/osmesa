package osmesa.analytics.stats

import osmesa.analytics._

import cats.implicits._
import com.monovore.decline._
import com.vividsolutions.jts.geom.Coordinate
import geotrellis.vector.{Feature, Line, Point}
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions._
import vectorpipe._

import java.math.BigDecimal
import java.time.Instant
import java.sql.Timestamp
import scala.collection.mutable
import scala.util.{Try, Success, Failure}

object CalculateStats {
  private implicit def changeTopicEncoder: Encoder[Array[StatTopic]] = ExpressionEncoder()

  case class Options(
    changesetPartitionCount: Int = 1000,
    wayPartitionCount: Int = 1000,
    nodePartitionCount: Int = 10000
  )

  object Options {
    def DEFAULT = Options()
  }

  def statTopics(col: Column): TypedColumn[Any, Array[StatTopic]] =
    udf[Array[StatTopic], Map[String, String]](StatTopics.tagsToTopics).
      apply(col).
      as[Array[StatTopic]]

  def computeChangesetStats(history: DataFrame, changesets: DataFrame, options: Options = Options.DEFAULT)(implicit ss: SparkSession): RDD[(Long, ChangesetStats)] = {
    import ss.implicits._

    val changesetPartitioner = new HashPartitioner(options.changesetPartitionCount)
    val wayPartitioner = new HashPartitioner(options.wayPartitionCount)
    val nodePartitioner = new HashPartitioner(options.nodePartitionCount)

    def generateUpstreamTopics[T <: OsmId](rdd: RDD[(Long, Iterable[(T, Long, Long, Set[StatTopic])])]): RDD[(Long, List[(T, UpstreamTopics)])] =
      rdd.
        mapValues { idsAndChangesets =>
          (idsAndChangesets.foldLeft(Map[T, Map[Long, (Set[StatTopic], Long)]]()) { case (acc, (osmId, changeset, version, statTopics)) =>
            acc.get(osmId) match {
              case Some(m) => acc + (osmId -> (m + (changeset -> (statTopics, version))))
              case _ => acc + (osmId -> Map(changeset -> (statTopics, version)))
            }
          }).toList.map { case (k, v) => (k, new UpstreamTopics(v)) }
        }

    // Create the base set of ChangesetStats we'll be joining against
    val initialChangesetStats =
      changesets.
        select($"id", $"uid", $"user", $"created_at", $"closed_at", $"tags", hashtags($"tags").alias("hashtags")).
        map { row =>
          val changeset = row.getAs[Long]("id")
          val userId = row.getAs[Long]("uid")
          val userName = row.getAs[String]("user")
          val createdAt = row.getAs[java.sql.Timestamp]("created_at")
          val closedAt = row.getAs[java.sql.Timestamp]("closed_at")
          val tags = row.getAs[Map[String, String]]("tags")
          val editor = tags.get("created_by")

          val hashtags = row.getAs[Seq[String]]("hashtags").toList

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
        }.
        partitionBy(changesetPartitioner)

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
          val topics = row.getAs[Seq[StatTopic]]("statTopics").toSet
          (changeset, StatCounter(osmId, changeset, version, topics))
        }

    val relationMembers =
      relevantRelations.
        select($"id", $"changeset", $"version", $"statTopics", explode($"members").as("member"))

    val relationsForWays: RDD[(Long, List[(RelationId, UpstreamTopics)])] =
      generateUpstreamTopics {
        relationMembers.
          where($"member.type" === "way").
          select($"id", $"changeset", $"version", $"statTopics", $"member.ref".as("wayId")).
          rdd.
          map { row =>
            val osmId = RelationId(row.getAs[Long]("id"))
            val changeset = row.getAs[Long]("changeset")
            val version = row.getAs[Long]("version")
            val topics = row.getAs[Seq[StatTopic]]("statTopics").toSet
            val wayId = row.getAs[Long]("wayId")

            (wayId, (osmId, changeset, version, topics))
          }.
          groupByKey(wayPartitioner)
      }

    val relationsForNodes: RDD[(Long, List[(RelationId, UpstreamTopics)])] =
      generateUpstreamTopics {
        relationMembers.
          where($"member.type" === "node").
          select($"id", $"changeset", $"version", $"statTopics", $"member.ref".as("nodeId")).
          rdd.
          map { row =>
            val osmId = RelationId(row.getAs[Long]("id"))
            val changeset = row.getAs[Long]("changeset")
            val version = row.getAs[Long]("version")
            val topics = row.getAs[Seq[StatTopic]]("statTopics").toSet
            val nodeId = row.getAs[Long]("nodeId")

            (nodeId, (osmId, changeset, version, topics))
          }.
          groupByKey(nodePartitioner)
      }

    val wayInfo =
      history.
        where("type == 'way'").
        select(
          $"id",
          $"changeset",
          $"version",
          $"timestamp",
          $"nds.ref".as("nodes"),
          statTopics($"tags").as("statTopics")
        ).
        rdd.
        map { row =>
          val id = row.getAs[Long]("id")
          val changeset = row.getAs[Long]("changeset")
          val version = row.getAs[Long]("version")
          val instant = row.getAs[Timestamp]("timestamp").toInstant.toEpochMilli
          val nodeIds = row.getAs[Seq[Long]]("nodes").toArray
          val topics = row.getAs[Seq[StatTopic]]("statTopics").toSet
          (id, (changeset, version, instant, nodeIds, topics))
        }

    val waysToRelations =
      wayInfo.
        leftOuterJoin(relationsForWays).
        filter { case (_, ((_, _, _, _, topics), relationOpt)) =>
          !topics.isEmpty || relationOpt.isDefined
        }

    val wayStatChanges =
      waysToRelations.
        map { case (wayId, ((changeset, version, _, nodeIds, topics), relationsOpt)) =>
          var statCounter = StatCounter()

          relationsOpt match {
            // List[(RelationId, Map[Long, Set[StatTopic]])]
            case Some(relations) =>
              for((relationId, upstreamTopics) <- relations) {
                val (upstreamTopicSet, isNew) = upstreamTopics.forChangeset(changeset)
                if(!upstreamTopicSet.isEmpty) {
                  statCounter = statCounter + (ChangeItem(relationId, changeset, isNew), upstreamTopicSet)
                }
              }
            case None => ()
          }

          if(!topics.isEmpty) {
            statCounter = statCounter + (ChangeItem(WayId(wayId), changeset, version == 1L), topics)
          }

          (changeset, statCounter)
        }

    val wayRelationsForNodes: RDD[(Long, List[(RelationId, UpstreamTopics)])] =
      generateUpstreamTopics {
        waysToRelations.
          flatMap { case (wayId, ((_, _, _, nodeIds, _), relationsOpt)) =>
            val relationTopics =
              relationsOpt match {
                case Some(relations) =>
                  (for(
                    (relationId, upstreamTopics) <- relations;
                    (changeset, (topics, version)) <- upstreamTopics.topicMap
                  ) yield {
                    (relationId, changeset, version, topics)
                  }).toList

                case None => List()
              }

            nodeIds map ((_, relationTopics))
          }
      }

    val waysForNodes: RDD[(Long, List[(WayId, UpstreamTopics)])] =
      generateUpstreamTopics {
        wayInfo.
          filter { case (_, (_, _, _, _, topics)) => !topics.isEmpty }.
          flatMap { case (wayId, (changeset, version, instant, nodeIds, topics)) =>
            nodeIds.map { nodeId =>
              (nodeId, (WayId(wayId), changeset, version, topics))
            }
          }.
          groupByKey(nodePartitioner)
      }

    val nodeInfo =
      history.
        where("type == 'node'").
        select(
          $"id",
          $"changeset",
          $"version",
          $"timestamp",
          $"lat",
          $"lon",
          statTopics($"tags").as("statTopics")
        ).
        rdd.
        map { row =>
          val id = row.getAs[Long]("id")
          val changeset = row.getAs[Long]("changeset")
          val version = row.getAs[Long]("version")
          val lat = Option(row.getAs[BigDecimal]("lat")).map(_.doubleValue).getOrElse(Double.NaN)
          val lon = Option(row.getAs[BigDecimal]("lon")).map(_.doubleValue).getOrElse(Double.NaN)
          val instant =
            row.getAs[Timestamp]("timestamp").toInstant.toEpochMilli
          val topics = row.getAs[Seq[StatTopic]]("statTopics").toSet
          (id, (changeset, version, instant, new Coordinate(lon, lat), topics))
        }

    val groupedNodes =
      nodeInfo.
        cogroup(relationsForNodes, waysForNodes, wayRelationsForNodes)// .

    val bcCountryLookup = ss.sparkContext.broadcast(new CountryLookup())

    val nodeStatChanges =
      groupedNodes.
        mapPartitions { partition =>
          val countryLookup = bcCountryLookup.value
          partition.
            flatMap { case (nodeId, (nodes, relations, ways, wayRelations)) =>
              for((changeset, version, _, coord, topics) <- nodes) yield {
                var statCounter = StatCounter()

                // Figure out the countries
                val countryOpt = countryLookup.lookup(coord)

                for((upstreamId, upstreamTopics) <- (relations ++ ways ++ wayRelations).flatten) {
                  val (upstreamTopicSet, isNew) = upstreamTopics.forChangeset(changeset)
                  if(!upstreamTopicSet.isEmpty) {
                    val item = ChangeItem(upstreamId, changeset, isNew)
                    statCounter = statCounter + (item, upstreamTopicSet)
                    countryOpt.foreach { country =>
                      statCounter = statCounter + country
                    }
                  }
                }

                if(!topics.isEmpty) {
                  val item = ChangeItem(NodeId(nodeId), changeset, version == 1L)
                  statCounter = statCounter + (item, topics)
                }

                countryOpt.foreach { country =>
                  statCounter = statCounter + country
                }

                (changeset, statCounter)
              }
            }
        }

    // Measure lenghts for roads and waterways

    val relevantNodesToWays: RDD[(Long, Iterable[(Long, Long, Long, Coordinate)])] =
      groupedNodes.
        filter { case (_, (_, _, ways, wayRelations)) =>
          (wayRelations ++ ways).
            flatten.
            foldLeft(false) { case (acc, (_, upstreamTopics)) =>
              acc || upstreamTopics.hasTopic(StatTopics.ROAD) || upstreamTopics.hasTopic(StatTopics.WATERWAY)
            }
        }.
        flatMap { case (nodeId, (nodes, relations, ways, wayRelations)) =>
          for(
            (wayId, _) <- ways.flatten;
            (changeset, _, instant, coord, _) <- nodes
          ) yield {
            (wayId.id, (nodeId, changeset, instant, coord))
          }
        }.
        groupByKey(wayPartitioner)

    val relevantWays: RDD[(Long, Iterable[(Array[Long], Long, Long, Map[StatTopic, Boolean])])] =
      waysToRelations.
        flatMap { case (wayId, ((changeset, version, instant, nodeIds, topics), relationsOpt)) =>
          val upstreams: Map[StatTopic, Boolean] = {
            val s =
              relationsOpt match {
                case Some(relations) =>
                  relations.
                    map { case (_, upstreamTopics) =>
                      upstreamTopics.forChangeset(changeset)._1
                    }.
                    reduce(_ ++ _)
                case None => Set()
              }
            s.map((_, false)).toMap
          }

          val topicsToNew =
            topics.map((_, version == 1L)).toMap

          val fullTopics =
            mergeMaps(upstreams, topicsToNew) { (w, r) => w }

          if(fullTopics.contains(StatTopics.ROAD) || fullTopics.contains(StatTopics.WATERWAY)) {
            Some((wayId, (nodeIds, changeset, instant, fullTopics)))
          } else {
            None
          }
        }.
        groupByKey(wayPartitioner)

    val wayLengthStatChanges: RDD[(Long, StatCounter)] =
      relevantNodesToWays.
        join(relevantWays).
        flatMap { case (wayId, (nodes, ways)) =>
          var changesetsToCounters =
            Map[Long, StatCounter]()

          WayLengthCalculator.calculateChangesetLengths(wayId, nodes, ways).
            foreach { case (changeset, (topic, lengths)) =>
              changesetsToCounters.get(changeset) match {
                case Some(counter) =>
                  changesetsToCounters =
                    changesetsToCounters + (changeset -> (counter + (topic, lengths)))
                case None =>
                  changesetsToCounters =
                    changesetsToCounters + (changeset -> (StatCounter() + (topic, lengths)))
              }
            }
          changesetsToCounters.toSeq
        }

    // Put it all together

    val mergedStatChanges =
      ss.sparkContext.union(
        relationStatChanges,
        wayStatChanges,
        nodeStatChanges,
        wayLengthStatChanges
      ).
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
              poisModified = counter.poisModified,
              kmRoadAdded = counter.roadsKmAdded,
              kmRoadModified = counter.roadsKmModified,
              kmWaterwayAdded = counter.waterwaysKmAdded,
              kmWaterwayModified = counter.waterwaysKmModified,
              countries = counter.countries
            )
          case None => stats
        }
      }
  }

  def computeUserStats(changesetStats: RDD[(Long, ChangesetStats)]): RDD[UserStats] =
    changesetStats
      .map { case (_, changesetStat) =>
        (changesetStat.userId, UserStats.fromChangesetStats(changesetStat))
      }
      .reduceByKey(_ merge _ )
      .map(_._2)

  def computeHashtagStats(changesetStats: RDD[(Long, ChangesetStats)]): RDD[HashtagStats] =
    changesetStats
      .flatMap { case (_, changesetStat) =>
        changesetStat.hashtags.map { hashtag =>
          (hashtag, HashtagStats.fromChangesetStats(hashtag, changesetStat))
        }
      }
      .reduceByKey(_ merge _)
      .map(_._2)

  def compute(history: DataFrame, changesets: DataFrame, options: Options = Options.DEFAULT)(implicit ss: SparkSession): (RDD[UserStats], RDD[HashtagStats]) = {
    val changesetStats = computeChangesetStats(history, changesets, options)

    (computeUserStats(changesetStats), computeHashtagStats(changesetStats))
  }
}
