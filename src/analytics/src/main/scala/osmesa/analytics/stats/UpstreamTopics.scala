package osmesa.analytics.stats

import osmesa.analytics._

class UpstreamTopics(val topicMap: Map[Long, Set[StatTopic]]) {
  private val (changesets, topics) = {
    val sorted = topicMap.toSeq.sortBy(_._1)
    (sorted.map(_._1).toArray, sorted.map(_._2).toArray)
  }

  /** Returns the topics for the upstream element
    * (way or relation) which was directly before the changeset parameter,
    * or the earliest changest for upstream element if the changeset passed in
    * is before the earliest changeset of the upstream element.
    */
  // def forChangeset(changeset: Long): Either[Long, Set[StatTopic]] = {
  //   val i = java.util.Arrays.binarySearch(changesets, changeset)
  //   if(i >= 0) { topics(i) }
  //   else {
  //     // For non matches, binarySearch returns the insertion index as -i - 1
  //     // We want the index before the insertion point.
  //     // If the insertion point is 0 (all changesets for the upstream change
  //     // are later than the changeset parameter), return no Topics.
  //     val insertionIndex = -i - 1
  //     if(insertionIndex > 0) { Right(topics(insertionIndex - 1)) }
  //     else { Left(changesets(0)) }
  //   }
  // }
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
