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

    def getLength(nodeArray: Array[Long], instant: Long): Double =
      nodeArray.
        toList.
        map { node =>
          val (instants, coords) = nodeMap(node)
          val idx = {
            // index of the search key, if it is contained in the array; otherwise, (-(insertion point) - 1).
            val i = Arrays.binarySearch(instants, instant)
            if(i >= 0) { i }
            else {
              ~i - 1
            }
          }

          coords(math.max(idx, 0))
        }.
        sliding(2).
        collect { case List(x,y) => (x,y) }.
        foldLeft(0.0) { case (acc, (c1, c2)) =>
          acc + Distance.kmBetween(c1.x, c1.y, c2.x, c2.y)
        }

    val wayArr = ways.toArray.sortBy(_._3)

    val (_, _, result) =
      (ways.map { t => (t._2, t._3) }.toSet ++ nodes.map { t => (t._2, t._3) }.toSet).
        toArray.
        sortBy(_._2).
        foldLeft((0, 0.0, Seq[(Long, (StatTopic, (Double, Double)))]())) { case ((wayIdx, prevLength, acc), (changeset, instant)) =>
          val wInstant = wayArr(wayIdx)._3

          if(instant < wInstant) {
            (wayIdx, prevLength, acc)
          } else {
            val newWayIdx =
              if(instant > wInstant) {
                if(wayIdx + 1 < wayArr.length) {
                  if(wayArr(wayIdx + 1)._3 <= instant) {
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

            val totalLength =
              getLength(wayArr(wayIdx)._1, instant)

            val editLength =
              math.abs(totalLength - prevLength)

            val changesetLengths =
              topics.
                map { case (t, n) =>
                  if(t == StatTopics.ROAD || t == StatTopics.WATERWAY) {
                    Some(
                      if(n && changeset == wChangeset) { (t, (editLength, 0.0)) }
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
          }
        }

    result
  }
}
