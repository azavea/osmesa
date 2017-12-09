package osmesa.analytics.stats

import osmesa.analytics._

import spire.syntax.cfor._

case class UpstreamInfo(version: Long, topics: Set[StatTopic])
case class UpstreamChangesetInfo(changeset: Long, topics: Set[StatTopic], isNew: Boolean)

/** Takes a map of changesets to a tuple of the upstream ID
  * and the topics to the upstream version that contributed those topics:
  * Map[changeset, (osmId, (topics, version))] */
class UpstreamInfoMap(val infoMap: Map[Long, UpstreamInfo]) {
  private lazy val (changesets, infos) = {
    val sorted =
      infoMap.
        toSeq.sortBy(_._1)
    (sorted.map(_._1).toArray, sorted.map(_._2).toArray)
  }

  private lazy val allTopics = infos.map(_.topics).reduce(_ ++ _)

  def hasTopic(topic: StatTopic): Boolean =
    allTopics.contains(topic)

  /** Returns the info for the upstream element
    * (way or relation) which was directly at or before the changeset parameter
    * and whether or not it's new in the changeset,
    * or None if the changeset is before the earliest changeset for this upstream element.
    */
  def forChangeset(changeset: Long): Option[UpstreamChangesetInfo] = {
    val i = java.util.Arrays.binarySearch(changesets, changeset)
    if(i >= 0) { Some(UpstreamChangesetInfo(changesets(i), infos(i).topics, infos(i).version == 1L))  }
    else {
      // For non matches, binarySearch returns the insertion index as -i - 1
      // We want the index before the insertion point.
      // If the insertion point is 0 (all changesets for the upstream change
      // are later than the changeset parameter), return no Topics.
      val insertionIndex = ~i
      if(insertionIndex > 0) {
        Some(UpstreamChangesetInfo(changesets(insertionIndex - 1), infos(insertionIndex - 1).topics, false))
      } else { None }
    }
  }
}
