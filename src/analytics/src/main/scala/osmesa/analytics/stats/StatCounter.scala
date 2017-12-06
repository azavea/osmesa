package osmesa.analytics.stats

import osmesa.analytics._

class StatCounter private (
  private val elements: Map[StatTopic, Set[ChangeItem]] = Map(),
  val countries: Set[CountryId] = Set()
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

  // def +(item: ChangeItem, countryId: CountryId): StatCounter =
  //   new StatCounter(
  //     elements,
  //     countries + countryId
  //     // countries.get(item) match {
  //     //   case Some(s) => countries + (item -> (s + countryId))
  //     //   case None => countries + (item -> Set(countryId))
  //     // }
  //   )
  def +(countryId: CountryId): StatCounter =
    new StatCounter(elements, countries + countryId)

  // def +(item: ChangeItem, countryId: CountryId): StatCounter =
  //   new StatCounter(
  //     elements,
  //     countries.get(item) match {
  //       case Some(m) =>
  //         m.get(countryId) match {
  //           case Some(count) =>
  //             countries + (item -> (m + (countryId -> (count + 1))))
  //           case None =>
  //             countries + (item -> (m + (countryId -> 1)))
  //         }
  //       case None => countries + (item -> Map(countryId -> 1))
  //     }
  //   )

  def merge(other: StatCounter): StatCounter =
    new StatCounter(
      mergeSetMaps(elements, other.elements),
      countries ++ other.countries
//      mergeMaps(countries, other.countries)
      // {
      //   (countries.toSeq ++ other.countries.toSeq).
      //     groupBy(_._1).
      //     map { case (changeItem, mps) =>
      //       (
      //         changeItem,
      //         mps.
      //           flatMap(_._2.toSeq).
      //           groupBy(_._1).
      //           map { case (countryId, vs) =>
      //             (countryId, vs.map(_._2).sum)
      //           }.
      //           toMap
      //       )
      //     }.
      //     toMap
      // }
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
//  def countries: Set[CountryId] = countries
    // countries.
    //   map(_._2).
    //   reduce(
    //   groupBy(_._1).
    //   map { case (c, (_, tally)) =>
    //     CountryCount(c, tally.sum)
    //   }.
    //   toList
}

object StatCounter {
  def apply(id: OsmId, changeset: Long, version: Long, statTopics: Set[StatTopic]): StatCounter =
    new StatCounter() + (ChangeItem(id, changeset, version == 1L), statTopics)

  def apply(): StatCounter =
    new StatCounter()
}
