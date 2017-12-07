package osmesa.analytics.data

import osmesa.analytics._
import osmesa.analytics.stats._

import org.apache.spark.sql._

case class ExpectedCounts(added: Int, modified: Int) {
  override def toString =
    s"($added, $modified)"
}
object ExpectedCounts {
  implicit def fromTuple(tup: (Int, Int)) = ExpectedCounts(tup._1, tup._2)
}

case class ExpectedLengths(added: Double, modified: Double) {
  override def toString =
    s"($added, $modified)"
}
object ExpectedLengths {
  implicit def fromTuple(tup: (Double, Double)) = ExpectedLengths(tup._1, tup._2)
}

case class ExpectedUserStats(
  user: TestData.User,
  buildings: ExpectedCounts = ExpectedCounts(0, 0),
  pois: ExpectedCounts = ExpectedCounts(0, 0),
  roads: ExpectedCounts = ExpectedCounts(0, 0),
  waterways: ExpectedCounts = ExpectedCounts(0, 0),
  roadsKm: ExpectedLengths = ExpectedLengths(0.0, 0.0),
  waterwaysKm: ExpectedLengths = ExpectedLengths(0.0, 0.0),
  countries: Set[String] = Set(), // Just names
  hashtags: Set[TestData.Hashtag] = Set()
) {
  override def toString: String =
    s"[User $user, buildings = $buildings, pois = $pois, roads = $roads $roadsKm, waterways = $waterways $waterwaysKm], countries = $countries, hashtags = $hashtags]"

  def updateTopic(topic: StatTopic, isNew: Boolean): ExpectedUserStats =
    topic match {
      case StatTopics.BUILDING =>
        if(isNew) {
          this.copy(buildings = buildings.copy(added = buildings.added + 1))
        } else {
          this.copy(buildings = buildings.copy(modified = buildings.modified + 1))
        }

      case StatTopics.ROAD =>
        if(isNew) {
          this.copy(roads = roads.copy(added = roads.added + 1))
        } else {
          this.copy(roads = roads.copy(modified = roads.modified + 1))
        }

      case StatTopics.POI =>
        if(isNew) {
          this.copy(pois = pois.copy(added = pois.added + 1))
        } else {
          this.copy(pois = pois.copy(modified = pois.modified + 1))
        }

      case StatTopics.WATERWAY =>
        if(isNew) {
          this.copy(waterways = waterways.copy(added = waterways.added + 1))
        } else {
          this.copy(waterways = waterways.copy(modified = waterways.modified + 1))
        }
    }
}

object ExpectedUserStats {
  def apply(s: UserStats): ExpectedUserStats = {
    ExpectedUserStats(
      user = TestData.User(s.uid, s.name),
      buildings = ExpectedCounts(s.buildingCountAdd, s.buildingCountMod),
      pois = ExpectedCounts(s.poiCountAdd, s.poiCountMod),
      roads = ExpectedCounts(s.roadCountAdd, s.roadCountMod),
      waterways = ExpectedCounts(s.waterwayCountAdd, 0),
      roadsKm = ExpectedLengths(s.kmRoadAdd, s.kmRoadMod),
      waterwaysKm = ExpectedLengths(s.kmWaterwayAdd, 0.0),
      countries = s.countries.map(_.id.name).toSet,
      hashtags = s.hashtags.map { t => TestData.Hashtag(t.tag) }.toSet
    )
  }
}

case class ExpectedHashtagStats(
  hashtag: TestData.Hashtag,
  buildings: ExpectedCounts = ExpectedCounts(0, 0),
  pois: ExpectedCounts = ExpectedCounts(0, 0),
  roads: ExpectedCounts = ExpectedCounts(0, 0),
  waterways: ExpectedCounts = ExpectedCounts(0, 0),
  roadsKm: ExpectedLengths = ExpectedLengths(0.0, 0.0),
  waterwaysKm: ExpectedLengths = ExpectedLengths(0.0, 0.0),
  users: Set[TestData.User] = Set(),
  totalEdits: Int = 0
) {
  override def toString: String =
    s"[Hashtag $hashtag, buildings = $buildings, pois = $pois, roads = $roads $roadsKm, waterways = $waterways $waterwaysKm], users = $users, totalEdits = $totalEdits]"

  def updateTopic(topic: StatTopic, isNew: Boolean): ExpectedHashtagStats =
    topic match {
      case StatTopics.BUILDING =>
        if(isNew) {
          this.copy(buildings = buildings.copy(added = buildings.added + 1))
        } else {
          this.copy(buildings = buildings.copy(modified = buildings.modified + 1))
        }

      case StatTopics.ROAD =>
        if(isNew) {
          this.copy(roads = roads.copy(added = roads.added + 1))
        } else {
          this.copy(roads = roads.copy(modified = roads.modified + 1))
        }

      case StatTopics.POI =>
        if(isNew) {
          this.copy(pois = pois.copy(added = pois.added + 1))
        } else {
          this.copy(pois = pois.copy(modified = pois.modified + 1))
        }

      case StatTopics.WATERWAY =>
        if(isNew) {
          this.copy(waterways = waterways.copy(added = waterways.added + 1))
        } else {
          this.copy(waterways = waterways.copy(modified = waterways.modified + 1))
        }
    }
}

object ExpectedHashtagStats {
  def apply(s: HashtagStats): ExpectedHashtagStats =
    ExpectedHashtagStats(
      hashtag = TestData.Hashtag(s.tag),
      buildings = ExpectedCounts(s.buildingsAdd, s.buildingsMod),
      pois = ExpectedCounts(s.poiAdd, 0),
      roads = ExpectedCounts(s.roadsAdd, s.roadsMod),
      waterways = ExpectedCounts(s.waterwayAdd, 0),
      roadsKm = ExpectedLengths(s.kmRoadAdd, s.kmRoadMod),
      waterwaysKm = ExpectedLengths(s.kmWaterwayAdd, 0.0),
      users = s.users.map { u => TestData.User(u.id, u.name) }.toSet,
      totalEdits = s.totalEdits.toInt
    )
}

trait OsmDataset {
  def history(implicit ss: SparkSession): DataFrame
  def changesets(implicit ss: SparkSession): DataFrame
  def calculatedStats: (Seq[ExpectedUserStats], Seq[ExpectedHashtagStats])
  def expectedStats: Option[(Seq[ExpectedUserStats], Seq[ExpectedHashtagStats])]
}

object OsmDataset {
  type BuildFunc =
    (TestData.Elements, Changes) => Option[(Seq[ExpectedUserStats], Seq[ExpectedHashtagStats])]

  def build(f: BuildFunc): OsmDataset = {
    val elements = new TestData.Elements
    val changes = new Changes
    val stats =
      f(elements, changes)

    new OsmDataset {
      def history(implicit ss: SparkSession) = TestData.createHistory(changes.historyRows)
      def changesets(implicit ss: SparkSession) = TestData.createChangesets(changes.changesetRows)
      def calculatedStats = changes.stats
      def expectedStats = stats
    }
  }
}
