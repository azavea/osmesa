package osmesa.analytics.stats

import osmesa.analytics._

class StatCounter private (
  private val elements: Map[StatTopic, Set[ChangeItem]] = Map(),
  val countries: Set[CountryId] = Set(),
  private val topicLengths: Map[StatTopic, (Double, Double)] = Map()
) extends Serializable {
  def +(item: ChangeItem, statTopics: Set[StatTopic]): StatCounter = {
    var e = elements
    for(statTopic <- statTopics) {
      elements.get(statTopic) match {
        case Some(changeItems) => e = e + (statTopic -> (changeItems + item))
        case None => e = e + (statTopic -> Set(item))
      }
    }
    new StatCounter(e, countries, topicLengths)
  }

  def +(countryId: CountryId): StatCounter =
    new StatCounter(elements, countries + countryId, topicLengths)

  /** Lengths are (Added, Modified) */
  def +(statTopic: StatTopic, lengths: (Double, Double)): StatCounter =
    new StatCounter(elements, countries,
      topicLengths.get(statTopic) match {
        case Some((a,m)) => topicLengths + (statTopic -> (lengths._1 + a, lengths._2 + m))
        case None => topicLengths + (statTopic -> lengths)
      }
    )

  def merge(other: StatCounter): StatCounter =
    new StatCounter(
      mergeMaps(elements, other.elements)(_ ++ _),
      countries ++ other.countries,
      mergeMaps(topicLengths, other.topicLengths) { (l1, l2) => (l1._1 + l2._1, l1._2 + l2._2) }
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

  private def addedLength(topic: StatTopic): Double =
    topicLengths.get(topic) match {
      case Some(lengths) => lengths._1
      case None => 0.0
    }

  private def modifiedLength(topic: StatTopic): Double =
    topicLengths.get(topic) match {
      case Some(lengths) => lengths._2
      case None => 0.0
    }

  def roadsAdded: Int = countNew(StatTopics.ROAD)
  def roadsModified: Int = countModified(StatTopics.ROAD)
  def buildingsAdded: Int = countNew(StatTopics.BUILDING)
  def buildingsModified: Int = countModified(StatTopics.BUILDING)
  def waterwaysAdded: Int = countNew(StatTopics.WATERWAY)
  def waterwaysModified: Int = countModified(StatTopics.WATERWAY)
  def poisAdded: Int = countNew(StatTopics.POI)
  def poisModified: Int = countModified(StatTopics.POI)

  def roadsKmAdded: Double = addedLength(StatTopics.ROAD)
  def roadsKmModified: Double = modifiedLength(StatTopics.ROAD)
  def waterwaysKmAdded: Double = addedLength(StatTopics.WATERWAY)
  def waterwaysKmModified: Double = modifiedLength(StatTopics.WATERWAY)

}

object StatCounter {
  def apply(id: OsmId, changeset: Long, version: Long, statTopics: Set[StatTopic]): StatCounter =
    new StatCounter() + (ChangeItem(id, changeset, version == 1L), statTopics)

  def apply(): StatCounter =
    new StatCounter()
}
