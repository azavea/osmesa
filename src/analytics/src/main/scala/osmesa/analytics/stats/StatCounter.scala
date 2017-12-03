package osmesa.analytics.stats

import osmesa.analytics._

class StatCounter private (
  private val elements: Map[StatTopic, Set[ChangeItem]] = Map(),
  private val countries: Map[ChangeItem, Set[CountryId]] = Map()
) {
  def +(item: ChangeItem, statTopics: Set[StatTopic]): StatCounter = {
    var e = elements
    for(statTopic <- statTopics) {
      elements.get(statTopic) match {
        case Some(changeItems) => e = e + (statTopic -> (changeItems + item))
        case None => e = e + (statTopic -> Set(item))
      }
    }
    new StatCounter(e, countries)
  }

  def +(item: ChangeItem, countryId: CountryId): StatCounter =
    new StatCounter(
      elements,
      countries.get(item) match {
        case Some(s) => countries + (item -> (s + countryId))
        case None => countries + (item -> Set(countryId))
      }
    )

  private def mergeMaps[K, V](m1: Map[K, Set[V]], m2: Map[K, Set[V]]): Map[K, Set[V]] =
    (m1.toSeq ++ m2.toSeq).
      groupBy(_._1).
      map { case (k, vs) =>
        (k, vs.map(_._2).reduce(_ ++ _))
      }.
      toMap

  def merge(other: StatCounter): StatCounter =
    new StatCounter(
      mergeMaps(elements, other.elements),
      mergeMaps(countries, other.countries)
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
