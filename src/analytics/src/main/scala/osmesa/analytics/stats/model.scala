package osmesa.analytics.stats

import osmesa.analytics._

import java.time._

sealed abstract class OsmId { def id: Long }
case class NodeId(id: Long) extends OsmId
case class WayId(id: Long) extends OsmId
case class RelationId(id: Long) extends OsmId

case class ChangeItem(osmId: OsmId, changeset: Long, isNew: Boolean)

case class HashtagCount(tag: String, count: Int)
case class EditorCount(editor: String, count: Int)
case class DayCount(day: LocalDate, count: Int)
case class UserCount(id: Long, name: String, count: Int)
case class CountryCount(id: CountryId, count: Int)

case class HashtagStats(
  /** Tag that represents this hashtag (lower case, without '#') */
  tag: String,

  /** ZXY template for vector tile set of the hashtag's extent. */
  extentUri: String,

  /** Number of ways or relations that are version=1 and linked changeset comments contains hashtag,
    * and  that have a 'highway' tag, and the value of that tag is one of:
    * "motorway", "trunk", "motorway_link", "trunk_link", "primary", "secondary", "tertiary",
    * "primary_link", "secondary_link", "tertiary_link", "service", "residential", "unclassified",
    * "living_street", or "road".
    */
  roadsAdd: Int,

  /** Number of ways or relations that are version=greater than 1 and linked changeset comments contains hashtag,
    * OR NODES HAVE CHANGED
    * and  that have a 'highway' tag, and the value of that tag is one of:
    * "motorway", "trunk", "motorway_link", "trunk_link", "primary", "secondary", "tertiary",
    * "primary_link", "secondary_link", "tertiary_link", "service", "residential", "unclassified",
    * "living_street", or "road".
    */
  roadsMod: Int,

  /** Number of ways or relations that are version=1 and linked changeset comments contains hashtag,
    * that have the tag "building="
    */
  buildingsAdd: Int,

  /** Number of ways or relations that are version=greater than 1 and linked changeset comments
    * contains hashtag,
    * OR NODES HAVE CHANGED
    * that have the tag "building="
    */
  buildingsMod: Int,

  /** Number of relations or ways that are version=1 and linked changeset comments contains hashtag,
    * and  that have a 'waterway=' tag
    */
  waterwayAdd: Int,

  /** Number of ways, nodes or relations that are version=1 and linked changeset comments contains hashtag,
    * and have the tag amenity=
    */
  poiAdd: Int,

  /** For the same criteria as "roadsAdd", the total KM distance between nodes between those ways. */
  kmRoadAdd: Double,

  /** For the same criteria as "roadsMod", the total KM distance between nodes between those ways. */
  kmRoadMod: Double,

  /** For the same criteria as "waterwayAdd", the total KM distance between nodes between those ways. */
  kmWaterwayAdd: Double,

  /** List of participating users */
  users: List[UserCount],

  /** Total number of changesets with this hashtag */
  totalEdits: Long
) {
  /** Merge two user objects to aggregate statistics.
    * Will throw if the IDs are not the same
    */
  def merge(other: HashtagStats): HashtagStats =
    if(tag != other.tag) sys.error(s"Hashtag IDs do not match, cannot aggregate: ${tag} != ${other.tag}")
    else {
      HashtagStats(
        tag,
        extentUri,
        roadsAdd = roadsAdd + other.roadsAdd,
        roadsMod = roadsMod + other.roadsMod,
        buildingsAdd = buildingsAdd + other.buildingsAdd,
        buildingsMod = buildingsMod + other.buildingsMod,
        waterwayAdd = waterwayAdd + other.waterwayAdd,
        poiAdd = poiAdd + other.poiAdd,
        kmRoadAdd = kmRoadAdd + other.kmRoadAdd,
        kmRoadMod = kmRoadMod + other.kmRoadMod,
        kmWaterwayAdd = kmWaterwayAdd + other.kmWaterwayAdd,
        users = mergeMaps(
          users.map { c => ((c.id, c.name), c.count) }.toMap,
          other.users.map { c => ((c.id, c.name), c.count) }.toMap
        )(_ + _).map { case (k, v) => UserCount(k._1, k._2, v) }.toList, // TODO: Cleanup
        totalEdits = totalEdits + other.totalEdits
      )
    }

  /** Temporary glue to core types that are served out by the stats server */
  def toCoreType: osmesa.common.model.Campaign =
    osmesa.common.model.Campaign(
      tag = tag,
      geoExtent = extentUri,
      roadCountAdd = roadsAdd,
      roadsCountMod = roadsMod,
      buildingCountAdd = buildingsAdd,
      buildingCountMod = buildingsMod,
      waterwayCountAdd = waterwayAdd,
      poiCountAdd = poiAdd,
      roadKmAdd = kmRoadAdd,
      roadKmMod = kmRoadMod,
      waterwayKmAdd = kmWaterwayAdd
    )
}

object HashtagStats {
  def hashtagExtentUri(hashtag: String): String =
    s"hashtags/${hashtag}/{z}/{x}/{y}.mvt"

  def fromChangesetStats(hashtag: String, changesetStats: ChangesetStats): HashtagStats =
    HashtagStats(
      hashtag,
      hashtagExtentUri(hashtag),
      roadsAdd = changesetStats.roadsAdded,
      roadsMod = changesetStats.roadsModified,
      buildingsAdd = changesetStats.buildingsAdded,
      buildingsMod = changesetStats.buildingsModified,
      waterwayAdd = changesetStats.waterwaysAdded,
      poiAdd = changesetStats.poisAdded,
      kmRoadAdd = changesetStats.kmRoadAdded,
      kmRoadMod = changesetStats.kmRoadModified,
      kmWaterwayAdd = changesetStats.kmWaterwayAdded,
      users = List(UserCount(changesetStats.userId, changesetStats.userName, 1)),
      totalEdits = 1L
    )
}

case class UserStats(
  /** UID of the user */
  uid: Long,

  /** Name of the user as per last import */
  name: String,

  /** ZXY template for vector tile set of the users's extent. */
  extent: String,

  buildingCountAdd: Int, // 2

  buildingCountMod: Int,

  poiCountAdd: Int,

  poiCountMod: Int,

  kmWaterwayAdd: Double,

  waterwayCountAdd: Int,

  /** ... */
  kmRoadAdd: Double,

  /** ... */
  kmRoadMod: Double,

  /** ... */
  roadCountAdd: Int,

  /** ... */
  roadCountMod: Int,

  /** Number of changesets that have uid = this user */
  changesetCount: Int,

  /** List of editors that are being used in the changesets. Counted by changeset.
    * Changeset contains a tag that is "created_by=".
    */
  editors: List[EditorCount],

  /** Changeset timestamps, counts by day. */
  editTimes: List[DayCount],

  /** Set of countries that contain nodes that this user has edited in, counted by changeset */
  countries: List[CountryCount],

  /** Set of hashtags this user has contributed to, counted by changeset */
  hashtags: List[HashtagCount]
) {
  /** Merge two user objects to aggregate statistics.
    * Will throw if the IDs are not the same
    */
  def merge(other: UserStats): UserStats =
    if(uid != other.uid) sys.error(s"User IDs do not match, cannot aggregate: ${uid} != ${other.uid}")
    else {
      UserStats(
        uid,
        name,
        extent,
        buildingCountAdd + other.buildingCountAdd,
        buildingCountMod + other.buildingCountMod,
        poiCountAdd + other.poiCountAdd,
        poiCountMod + other.poiCountMod,
        kmWaterwayAdd + other.kmWaterwayAdd,
        waterwayCountAdd + other.waterwayCountAdd,
        kmRoadAdd + other.kmRoadAdd,
        kmRoadMod + other.kmRoadMod,
        roadCountAdd + other.roadCountAdd,
        roadCountMod + other.roadCountMod,
        changesetCount + other.changesetCount,
        mergeMaps(
          editors.map { c => (c.editor, c.count) }.toMap,
          other.editors.map { c => (c.editor, c.count) }.toMap
        )(_ + _).map { case (k, v) => EditorCount(k, v) }.toList, // TODO: Cleanup
        mergeMaps(
          editTimes.map { c => (c.day, c.count) }.toMap,
          other.editTimes.map { c => (c.day, c.count) }.toMap
        )(_ + _).map { case (k, v) => DayCount(k, v) }.toList, // TODO: Cleanup
        mergeMaps(
          countries.map { c => (c.id, c.count) }.toMap,
          other.countries.map { c => (c.id, c.count) }.toMap
        )(_ + _).map { case (k, v) => CountryCount(k, v) }.toList, // TODO: Cleanup
        mergeMaps(
          hashtags.map { c => (c.tag, c.count) }.toMap,
          other.hashtags.map { c => (c.tag, c.count) }.toMap
        )(_ + _).map { case (k, v) => HashtagCount(k, v) }.toList // TODO: Cleanup
      )
    }

  /** Temporary glue to core types that are served out by the stats server */
  def toCoreType: osmesa.common.model.User =
    osmesa.common.model.User(
      uid = uid,
      name = name,
      geoExtent = extent,
      buildingCountAdd = buildingCountAdd,
      buildingCountMod = buildingCountMod,
      poiCountAdd = poiCountAdd,
      waterwayKmAdd = kmWaterwayAdd,
      waterwayCountAdd = waterwayCountAdd,
      roadKmAdd = kmRoadAdd,
      roadKmMod = kmRoadMod,
      roadCountAdd = roadCountAdd,
      roadCountMod = roadCountMod,
      changesetCount = changesetCount,
      editCount = changesetCount,
      editTimes = editTimes.map { case DayCount(instant, count) =>
        osmesa.common.model.Day(instant, count)
      },
      countryList = countries.map { case CountryCount(CountryId(name, _), count) =>
        osmesa.common.model.Country(name, count)
      },
      hashtags = hashtags.map { case HashtagCount(tag, count) =>
        osmesa.common.model.Hashtag(tag, count)
      }
    )
}

object UserStats {
  def userExtentUri(userId: Long): String =
    s"users/${userId}/{z}/{x}/{y}.mvt"

  def fromChangesetStats(changesetStats: ChangesetStats): UserStats =
    UserStats(
      changesetStats.userId,
      changesetStats.userName,
      userExtentUri(changesetStats.userId),
      changesetStats.buildingsAdded,
      changesetStats.buildingsModified,
      changesetStats.poisAdded,
      changesetStats.poisModified,
      changesetStats.kmWaterwayAdded,
      changesetStats.waterwaysAdded,
      changesetStats.kmRoadAdded,
      changesetStats.kmRoadModified,
      changesetStats.roadsAdded,
      changesetStats.roadsModified,
      1,
      changesetStats.editor.map(EditorCount(_, 1)).toList,
      List(DayCount(changesetStats.closedAt.atOffset(ZoneOffset.UTC).toLocalDate, 1)),
      changesetStats.countries.map(CountryCount(_, 1)).toList,
      changesetStats.hashtags.map(HashtagCount(_, 1))
    )
}

case class ChangesetStats(
  changeset: Long,
  userId: Long,
  userName: String,
  createdAt: Instant,
  closedAt: Instant,
  editor: Option[String],
  hashtags: List[String],
  roadsAdded: Int = 0, // ways or relations added
  roadsModified: Int = 0, // ways, identified by (ways, relations)
  buildingsAdded: Int = 0,// ways or relations added
  buildingsModified: Int = 0, // ways, identified by (ways, relations)
  waterwaysAdded: Int = 0, // ways or relations added
  waterwaysModified: Int = 0, // ways, identified by (ways, relations)
  poisAdded: Int = 0, // nodes, ways or relations added
  poisModified: Int = 0,
  kmRoadAdded: Double = 0.0,
  kmRoadModified: Double = 0.0,
  kmWaterwayAdded: Double = 0.0,
  kmWaterwayModified: Double = 0.0,
  countries: Set[CountryId] = Set()
)
