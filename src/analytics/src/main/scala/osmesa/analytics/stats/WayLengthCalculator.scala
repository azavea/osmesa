package osmesa.analytics.stats

import com.vividsolutions.jts.geom.Coordinate

import java.util.Arrays

object WayLengthCalculator {
  /** Returns Seq[(changeset, (topic, (added, modified)))] */
  def calculateChangesetLengths(
    nodes: Iterable[(Long, Long, Coordinate)],
    ways: Iterable[(Array[Long], Long, Map[StatTopic, Boolean])]
  ): Seq[(Long, (StatTopic, (Double, Double)))] = {

    // Construct a map of nodes -> arrays that let us do binary search for
    // the correct coord given a changeset. We want the coordinate that
    // is changed most recently before (or during) the given changeset.
    val nodeMap: Map[Long, (Array[Long], Array[Coordinate])] =
      nodes.
        groupBy(_._1).
        map { case (nodeId, vs) =>
          val (changesets, coords) =
            vs.
              map { case (_, changeset, coord) => (changeset, coord) }.
              toArray.
              sortBy(_._1).
              unzip

          (nodeId, (changesets, coords))
        }.
        toMap

    def getLength(nodeArray: Array[Long], changeset: Long): Double =
      nodeArray.
        toList.
        map { node =>
          val (changesets, coords) = nodeMap(node)
          val idx = {
            // index of the search key, if it is contained in the array; otherwise, (-(insertion point) - 1).
            val i = Arrays.binarySearch(changesets, changeset)
            if(i >= 0) { i }
            else {
              ~i - 1
            }
          }
          if(idx < 0) {
            sys.error(
              "Unexpected: way contains node where way changeset is less than lowest node changeset. " +
                s"NODE: ${node}. CHANGESETS ${changesets.toSeq}. REQ CHANGESET ${changeset}."
            )
          }
          coords(idx)
        }.
        sliding(2).
        collect { case List(x,y) => (x,y) }.
        foldLeft(0.0) { case (acc, (c1, c2)) =>
          acc + Distance.kmBetween(c1.x, c1.y, c2.x, c2.y)
        }

    val wayArr = ways.toArray.sortBy(_._2)

    val (_, _, result) =
      (ways.map(_._2).toSet ++ nodes.map(_._2).toSet).
        toArray.
        sorted.
        foldLeft((0, 0.0, Seq[(Long, (StatTopic, (Double, Double)))]())) { case ((wayIdx, prevLength, acc), changeset) =>
          val wChangeset = wayArr(wayIdx)._2

          if(changeset < wChangeset) {
            (wayIdx, prevLength, acc)
          } else {
            val newWayIdx =
              if(changeset > wChangeset) {
                if(wayIdx + 1 < wayArr.length) {
                  if(wayArr(wayIdx)._2 <= changeset) {
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

            val topics = wayArr(wayIdx)._3

            val totalLength =
              getLength(wayArr(wayIdx)._1, changeset)

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
