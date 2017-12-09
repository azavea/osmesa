package osmesa.analytics.stats

import com.vividsolutions.jts.geom.Coordinate

import java.util.Arrays

object WayLengthCalculator {
  /** Cacluates the sequence of lengths added or modified for ROADS or WATERWAYS.
    * nodes are tuples of [(id, changeset, instant, coordinate)]
    * ways are tuples of [(nodeIds, changset, instant, topicToIsNew)]
    *
    * instant = milliseconds from epoch
    *
    * Returns Seq[(changeset, (topic, (added, modified)))]
    */
  def calculateChangesetLengths(
    wayId: Long,
    nodes: Iterable[(Long, Long, Long, Coordinate)],
    ways: Iterable[(Array[Long], Long, Long, Map[StatTopic, Boolean])]
  ): Seq[(Long, (StatTopic, (Double, Double)))] = {

    // Construct a map of nodes -> arrays that let us do binary search for
    // the correct coord given an instant. We want the coordinate that
    // is changed most recently before (or during) the given changeset.
    val nodeMap: Map[Long, (Array[Long], Array[Coordinate])] =
      nodes.
        groupBy(_._1).
        map { case (nodeId, vs) =>
          val (instants, coords) =
            vs.
              map { case (_, _, instant, coord) => (instant, coord) }.
              toArray.
              sortBy(_._1).
              unzip

          (nodeId, (instants, coords))
        }.
        toMap

    // To be safe and not blow up the job on bad data, if there are any
    // nodes the way is pointing to that we don't have in `nodes`, just skip
    // this version of the way.
    def getLength(nodeArray: Array[Long], instant: Long): Option[Double] = {
      val coordsOpt =
        nodeArray.
          toList.
          map { node =>
            nodeMap.get(node).
              flatMap { case (instants, coords) =>
                val idx = {
                  // index of the search key, if it is contained in the array; otherwise, (-(insertion point) - 1).
                  val i = Arrays.binarySearch(instants, instant)
                  if(i >= 0) { i }
                  else {
                    ~i - 1
                  }
                }

                val c = coords(math.max(idx, 0))
                // Sometimes nodes can have NaN values in the ORC, which comes from missing values,
                // e.g. node 1647000416
                if(java.lang.Double.isNaN(c.x) || java.lang.Double.isNaN(c.y)) { None }
                else { Some(c) }
              }
          }

      if(coordsOpt.foldLeft(true)(_ && _.isDefined)) {
        Some(
          coordsOpt.
            flatten.
            sliding(2).
            collect { case List(x,y) => (x,y) }.
            foldLeft(0.0) { case (acc, (c1, c2)) =>
              val dist = Distance.kmBetween(c1.x, c1.y, c2.x, c2.y)
              acc + dist
            }
        )
      } else { None }
    }

    val wayArr = ways.toArray.sortBy { case (_, changeset, instant, _) => (instant, changeset) }

    val (_, _, result) =
      (ways.map { t => (t._2, t._3) }.toSet ++ nodes.map { t => (t._2, t._3) }.toSet).
        toArray.
        sortBy(_._2).
        foldLeft((0, 0.0, Seq[(Long, (StatTopic, (Double, Double)))]())) { case ((wayIdx, prevLength, acc), (changeset, instant)) =>
          val wInstant = wayArr(wayIdx)._3

          if(instant < wInstant && wayArr(wayIdx)._2 != changeset) {
            // Skip a node change if it's not attached to a way yet.
            (wayIdx, prevLength, acc)
          } else {
            val newWayIdx =
              if(instant > wInstant) {
                // If the target instant (e.g. from a node change) is greater or equal than this way instant,
                // and is also greater than or equal to the next way instant, shift the way index
                // up one and start working with the new way.
                if(wayIdx + 1 < wayArr.length) {
                  if(wayArr(wayIdx + 1)._3 <= instant || changeset == wayArr(wayIdx + 1)._2) {
                    wayIdx + 1
                  } else {
                    wayIdx
                  }
                } else {
                  wayIdx
                }
              } else {
                wayIdx
              }

            val wChangeset = wayArr(wayIdx)._2
            val topics = wayArr(wayIdx)._4

            getLength(wayArr(wayIdx)._1, instant) match {
              case Some(totalLength) =>
                val editLength =
                  math.abs(totalLength - prevLength)

                val changesetLengths =
                  topics.
                    map { case (t, isNew) =>
                      if(t == StatTopics.ROAD || t == StatTopics.WATERWAY) {
                        Some(
                          if(isNew && changeset == wChangeset) { (t, (editLength, 0.0)) }
                          else { (t, (0.0, editLength)) }
                        )
                      } else {
                        None
                      }
                    }.
                    flatten.
                    toMap.
                    map { case (k, v) =>
                      (changeset, (k, v))
                    }.
                    toSeq

                (newWayIdx, totalLength, changesetLengths ++ acc)
              case None =>
                (newWayIdx, prevLength, acc)
            }
          }
        }
    result
  }
}
