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
    * Computes the length differences based on changests. This means if there
    * are more than one version of the way in a single changeset, this will
    * only consider the second way and the difference of length between
    * that second way version and a previous version from a prior changeset.
    * This will account for nodes that change without the way version changing,
    * where the length difference will be credited to the changeset that modified
    * the participating node(s).
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

    val (wayInstants, waysArr) =
      ways.
        toArray.
        sortBy(_._3).
        map { case (nodeIds, changeset, instant, topicMap) =>
          (instant, (nodeIds, changeset, topicMap))
        }.
        unzip

    /** Gets the nodes and topics for a way given an instant. The way chosen will be
      * the most recent way that is <= the instant.
      * If the instant is before all instants of the way, return None.
      */
    def getWayForInstant(instant: Long): Option[(Array[Long], Long, Map[StatTopic, Boolean])] = {
      val idx = {
        // index of the search key, if it is contained in the array; otherwise, (-(insertion point) - 1).
        val i = Arrays.binarySearch(wayInstants, instant)
        if(i >= 0) { i }
        else {
          ~i - 1
        }
      }

      if(idx < 0) { None }
      else { Some(waysArr(idx)) }
    }

    /** Gets the length of this way at an instant.
      *  To be safe and not blow up the job on bad data, if there are any
      * nodes the way is pointing to that we don't have in `nodes`, just skip
      * this version of the way.
      */
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

    val changesetsToMaxInstants =
      (ways.map { t => (t._2, t._3) }.toSet ++ nodes.map { t => (t._2, t._3) }.toSet).
        groupBy(_._1).
        map { case (changeset, vs) =>
          (changeset, vs.map(_._2).max)
        }.
        toSeq.
        sortBy(_._2)

    // Accumulator is (previous way length, changesets -> (topic -> (addedLength, modifiedLength)).
    val seed =
      (0.0, Seq[(Long, (StatTopic, (Double, Double)))]())

    val (_, result) =
      changesetsToMaxInstants.
        foldLeft(seed) { case ((prevLength, acc), (changeset, changesetInstant)) =>
          (for(
            (nodes, wayChangeset, topics) <- getWayForInstant(changesetInstant);
            length <- getLength(nodes, changesetInstant)
          ) yield {
            val editLength =
              math.abs(length - prevLength)

            val changesetLengths =
              topics.
                map { case (t, isNew) =>
                  if(t == StatTopics.ROAD || t == StatTopics.WATERWAY) {
                    Some(
                      if(isNew && changeset == wayChangeset) { (t, (editLength, 0.0)) }
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

            (length, changesetLengths ++ acc)
          }).getOrElse(
            (prevLength, acc)
          )
        }

    result
  }
}
