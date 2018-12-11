package osmesa.common

import java.io._
import java.sql.Timestamp

import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json._
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.jts.GeometryUDT
import org.apache.spark.sql.types._
import org.locationtech.geomesa.spark.jts._
import org.locationtech.jts.{geom => jts}
import osmesa.common.functions.osm._
import osmesa.common.relations.MultiPolygons
import osmesa.common.relations.Routes
import osmesa.common.util.Caching
import spray.json._

object ProcessOSM {
  val NodeType: Byte = 1
  val WayType: Byte = 2
  val RelationType: Byte = 3
  val MultiPolygonRoles: Seq[String] = Set("", "outer", "inner").toSeq

  lazy val logger: Logger = Logger.getLogger(getClass)

  lazy val BareElementSchema = StructType(
    StructField("changeset", LongType, nullable = false) ::
      StructField("id", LongType, nullable = false) ::
      StructField("version", IntegerType, nullable = false) ::
      StructField("updated", TimestampType, nullable = false) ::
      StructField("geom", GeometryUDT) ::
      Nil)

  lazy val BareElementEncoder: Encoder[Row] = RowEncoder(BareElementSchema)

  lazy val TaggedVersionedElementSchema = StructType(
    StructField("changeset", LongType, nullable = false) ::
      StructField("id", LongType, nullable = false) ::
      StructField("tags", MapType(StringType, StringType, valueContainsNull = false), nullable = false) ::
      StructField("version", IntegerType, nullable = false) ::
      StructField("minorVersion", IntegerType, nullable = false) ::
      StructField("updated", TimestampType, nullable = false) ::
      StructField("validUntil", TimestampType) ::
      StructField("geom", GeometryUDT) ::
      Nil)

  lazy val TaggedVersionedElementEncoder: Encoder[Row] = RowEncoder(TaggedVersionedElementSchema)

  lazy val VersionedElementSchema = StructType(
    StructField("changeset", LongType, nullable = false) ::
      StructField("id", LongType, nullable = false) ::
      StructField("version", IntegerType, nullable = false) ::
      StructField("minorVersion", IntegerType, nullable = false) ::
      StructField("updated", TimestampType, nullable = false) ::
      StructField("validUntil", TimestampType) ::
      StructField("geom", GeometryUDT) ::
      Nil)

  lazy val VersionedElementEncoder: Encoder[Row] = RowEncoder(VersionedElementSchema)

  /**
    * Snapshot pre-processed elements.
    *
    * @param df        Elements (including 'validUntil column)
    * @param timestamp Optional timestamp to snapshot at
    * @return DataFrame containing valid elements at timestamp (or now)
    */
  def snapshot(df: DataFrame, timestamp: Timestamp = null): DataFrame = {
    import df.sparkSession.implicits._

    df
      .where(
        'updated <= coalesce(lit(timestamp), current_timestamp)
          and coalesce(lit(timestamp), current_timestamp) < coalesce('validUntil, date_add(current_timestamp, 1)))
  }

  /**
    * Pre-process nodes. Copies coordinates + tags from versions prior to being deleted (!'visible), as they're cleared
    * out otherwise, and adds 'validUntil based on the creation timestamp of the next version. Nodes with null
    * 'validUntil values are currently valid.
    *
    * @param history DataFrame containing nodes.
    * @return processed nodes.
    */
  def preprocessNodes(history: DataFrame, extent: Option[Extent] = None): DataFrame = {
    import history.sparkSession.implicits._

    val filteredHistory =
      extent match {
        case Some(e) =>
          history
            .where('lat > e.ymin and 'lat < e.ymax)
            .where('lon > e.xmin and 'lon < e.xmax)
        case None =>
          history
      }

    if (filteredHistory.columns.contains("validUntil")) {
      filteredHistory
    } else {
      @transient val idByVersion = Window.partitionBy('id).orderBy('version)

      // when a node has been deleted, it doesn't include any tags; use a window function to retrieve the last tags
      // present and use those
      filteredHistory
        .where('type === "node")
        .repartition('id)
        .select(
          'id,
          when(!'visible and (lag('tags, 1) over idByVersion).isNotNull,
            lag('tags, 1) over idByVersion)
            .otherwise('tags) as 'tags,
          when(!'visible, lit(Double.NaN)).otherwise('lat) as 'lat,
          when(!'visible, lit(Double.NaN)).otherwise('lon) as 'lon,
          'changeset,
          'timestamp,
          (lead('timestamp, 1) over idByVersion) as 'validUntil,
          'uid,
          'user,
          'version,
          'visible,
          !((lag('lat, 1) over idByVersion) <=> 'lat and (lag('lon, 1) over idByVersion) <=> 'lon) as 'geometryChanged
        )
    }
  }

  /**
    * Pre-process ways. Copies tags from versions prior to being deleted (!'visible), as they're cleared out
    * otherwise, dereferences 'nds.ref, and adds 'validUntil based on the creation timestamp of the next version. Ways
    * with null 'validUntil values are currently valid.
    *
    * @param history DataFrame containing ways.
    * @return processed ways.
    */
  def preprocessWays(history: DataFrame): DataFrame = {
    import history.sparkSession.implicits._

    if (history.columns.contains("validUntil")) {
      history
    } else {
      @transient val idByVersion = Window.partitionBy('id).orderBy('version)

      // when a node has been deleted, it doesn't include any tags; use a window function to retrieve the last tags
      // present and use those
      history
        .where('type === "way")
        .repartition('id)
        .select(
          'id,
          when(!'visible and (lag('tags, 1) over idByVersion).isNotNull,
            lag('tags, 1) over idByVersion)
            .otherwise('tags) as 'tags,
          $"nds.ref" as 'nds,
          'changeset,
          'timestamp,
          (lead('timestamp, 1) over idByVersion) as 'validUntil,
          'uid,
          'user,
          'version,
          'visible,
          !((lag('nds, 1) over idByVersion) <=> 'nds) as 'geometryChanged
        )
    }
  }

  /**
    * Pre-process relations. Copies tags from versions prior to being deleted (!'visible), as they're cleared out
    * otherwise, and adds 'validUntil based on the creation timestamp of the next version. Relations with null
    * 'validUntil values are currently valid.
    *
    * @param history DataFrame containing relations.
    * @return processed relations.
    */
  def preprocessRelations(history: DataFrame): DataFrame = {
    import history.sparkSession.implicits._

    if (history.columns.contains("validUntil")) {
      history
    } else {
      @transient val idByUpdated = Window.partitionBy('id).orderBy('version)

      // when an element has been deleted, it doesn't include any tags; use a window function to retrieve the last tags
      // present and use those
      history
        .where('type === "relation")
        .repartition('id)
        .select(
          'id,
          when(!'visible and (lag('tags, 1) over idByUpdated).isNotNull,
            lag('tags, 1) over idByUpdated)
            .otherwise('tags) as 'tags,
          compressMemberTypes('members) as 'members,
          'changeset,
          'timestamp,
          (lead('timestamp, 1) over idByUpdated) as 'validUntil,
          'uid,
          'user,
          'version,
          'visible)
    }
  }

  /**
    * Construct all geometries for a set of elements.
    *
    * @param elements DataFrame containing node, way, and relation elements
    * @return DataFrame containing geometries.
    */
  def constructGeometries(elements: DataFrame): DataFrame = {
    import elements.sparkSession.implicits._
    val st_pointToGeom = org.apache.spark.sql.functions.udf { pt: jts.Point => pt.asInstanceOf[jts.Geometry] }

    val nodes = ProcessOSM.preprocessNodes(elements)

    val nodeGeoms = ProcessOSM.constructPointGeometries(nodes)
      .withColumn("minorVersion", lit(0))
      .withColumn("geom", st_pointToGeom('geom))

    val wayGeoms = ProcessOSM.reconstructWayGeometries(elements, nodes)

    val relationGeoms = ProcessOSM.reconstructRelationGeometries(elements, wayGeoms)

    nodeGeoms
      .union(wayGeoms.where(size('tags) > 0).drop('geometryChanged))
      .union(relationGeoms)
  }

  /**
    * Construct point geometries. "Uninteresting" nodes are not included, so this is not suitable for way/relation
    * assembly.
    *
    * @param nodes DataFrame containing nodes
    * @return Nodes as Point geometries
    */
  def constructPointGeometries(nodes: DataFrame): DataFrame = {
    import nodes.sparkSession.implicits._

    val ns = preprocessNodes(nodes)
      .where(size('tags) > 0)

    ns
      // fetch the last version of a node within a single changeset
      .select('changeset, 'id, 'version, 'timestamp)
      .groupBy('changeset, 'id)
      .agg(max('version).cast(IntegerType) as 'version, max('timestamp) as 'updated)
      .join(ns.drop('changeset), Seq("id", "version"))
      .select(
        lit(NodeType) as '_type,
        'id,
        when('lon.isNotNull and 'lat.isNotNull, st_makePoint('lon, 'lat)) as 'geom,
        'tags,
        'changeset,
        'updated,
        'validUntil,
        'visible,
        'version)
  }

  /**
    * Reconstruct way geometries.
    *
    * Nodes and ways contain implicit timestamps that will be used to generate minor versions of geometry that they're
    * associated with (each entry that exists within a changeset).
    *
    * @param _ways        DataFrame containing ways to reconstruct.
    * @param _nodes       DataFrame containing nodes used to construct ways.
    * @param _nodesToWays Optional lookup table.
    * @return Way geometries.
    */
  def reconstructWayGeometries(_ways: DataFrame, _nodes: DataFrame, _nodesToWays: Option[DataFrame] = None)(implicit
                                                                                                            cache: Caching = Caching.none, cachePartitions: Option[Int] = None): DataFrame = {
    implicit val ss: SparkSession = _ways.sparkSession
    import ss.implicits._
    ss.withJTS

    @transient val idByVersion = Window.partitionBy('id).orderBy('version)

    val nodes = preprocessNodes(_nodes)
      // no longer correct after filtering out unchanged geometries
      .drop('validUntil)
      .where('geometryChanged)
      .drop('geometryChanged)
      // re-calculate validUntil windows
      .withColumn("validUntil", lead('timestamp, 1) over idByVersion)

    val ways = preprocessWays(_ways)
      .withColumn("isArea", isArea('tags))

    // Create (or re-use) a lookup table for node → ways
    val nodesToWays = _nodesToWays.getOrElse[DataFrame] {
      ways
        .select(explode('nds) as 'id, 'id as 'wayId, 'version, 'timestamp, 'validUntil)
    }

    // Create a way entry for each changeset in which a node was modified, containing the timestamp of the node that
    // triggered the association. This will later be used to assemble ways at each of those points in time. If you need
    // authorship, join on changesets
    // TODO check on partitioning of nodes (assume that the thing requesting the join gets to keep its partitioning)
    val waysByChangeset = nodes
      .select('changeset, 'id, 'timestamp as 'updated)
      .join(nodesToWays, Seq("id"))
      .where('timestamp <= 'updated and 'updated < coalesce('validUntil, current_timestamp))
      .select('changeset, 'wayId as 'id, 'version, 'updated)

    val allWayVersions = waysByChangeset
      // Union with raw ways to include those in the time line (if they weren't already triggered by node modifications
      // at the same time)
      .union(ways.select('changeset, 'id, 'version, 'timestamp as 'updated))
      // If a node and a way were modified within the same changeset at different times, there will be multiple entries
      // per changeset (with different timestamps). There should only be one per changeset.
      .groupBy('changeset, 'id)
      .agg(max('version).cast(IntegerType) as 'version, max('updated) as 'updated)
      .join(ways.select('id, 'version, 'nds, 'isArea), Seq("id", "version"))

    val explodedWays = allWayVersions
      .select('changeset, 'id, 'version, 'updated, 'isArea, posexplode_outer('nds) as Seq("idx", "ref"))
      // repartition including updated timestamp to avoid skew (version is insufficient, as
      // multiple instances may exist with the same version)
      .repartition('id, 'updated)

    val waysAndNodes = explodedWays
      .join(nodes.select('id as 'ref, 'timestamp, 'validUntil, 'lat, 'lon), Seq("ref"), "left_outer")
      .where('timestamp <= 'updated and 'updated < coalesce('validUntil, current_timestamp))

    implicit val encoder: Encoder[Row] = BareElementEncoder

    val wayGeoms = waysAndNodes
      .select('changeset, 'id, 'version, 'updated, 'isArea, 'idx, 'lat, 'lon)
      .groupByKey(row =>
        (row.getAs[Long]("changeset"), row.getAs[Long]("id"), row.getAs[Integer]("version"), row.getAs[Timestamp]("updated"))
      )
      .mapGroups {
        case ((changeset, id, version, updated), rows) =>
          val nds = rows.toVector
          val isArea = nds.head.getAs[Boolean]("isArea")
          val geom = nds
            .sortWith((a, b) => a.getAs[Int]("idx") < b.getAs[Int]("idx"))
            .map { row =>
              Seq(Option(row.get(row.fieldIndex("lon"))).map(_.asInstanceOf[Double]).getOrElse(Double.NaN),
                  Option(row.get(row.fieldIndex("lat"))).map(_.asInstanceOf[Double]).getOrElse(Double.NaN))
            } match {
              // no coordinates provided
              case coords if coords.isEmpty => Some(GeomFactory.factory.createLineString(Array.empty[jts.Coordinate]))
              // some of the coordinates are empty; this is invalid
              case coords if coords.exists(Option(_).isEmpty) => None
              // some of the coordinates are invalid
              case coords if coords.exists(_.exists(_.isNaN)) => None
              // 1 pair of coordinates provided
              case coords if coords.length == 1 =>
                Some(GeomFactory.factory.createPoint(new jts.Coordinate(coords.head.head, coords.head.last)))
              case coords => {
                val coordinates = coords.map(xy => new jts.Coordinate(xy.head.toDouble, xy.last.toDouble)).toArray
                val line = GeomFactory.factory.createLineString(coordinates)

                if (isArea && line.getNumPoints >= 4 && line.isClosed)
                  Some(GeomFactory.factory.createPolygon(line.getCoordinateSequence))
                else
                  Some(line)
              }
            }
          val geometry = geom match {
            case Some(g) if g.isValid => g
            case _ => null
          }
          new GenericRowWithSchema(Array(changeset, id, version, updated, geometry), BareElementSchema): Row
      }

    @transient val idAndVersionByUpdated = Window.partitionBy('id, 'version).orderBy('updated)
    @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

    wayGeoms
      // Assign `minorVersion` and rewrite `validUntil` to match
      .withColumn("validUntil", lead('updated, 1) over idByUpdated)
      .withColumn("minorVersion", (row_number over idAndVersionByUpdated) - 1)
      .withColumn("geometryChanged", !((lag('geom, 1) over idByUpdated) <=> 'geom))
      .join(ways.select('id, 'version, 'tags, 'visible), Seq("id", "version"))
      .select(
        lit(WayType) as '_type,
        'id,
        'geom,
        'tags,
        'changeset,
        'updated,
        'validUntil,
        'visible,
        'version,
        'minorVersion,
        'geometryChanged)
  }

  private def getRelationMembers(relations: DataFrame, geoms: DataFrame) = {
    implicit val ss: SparkSession = relations.sparkSession
    import ss.implicits._

    // way to relation lookup table (missing 'role, since it's not needed here)
    val waysToRelations = relations
      .select(explode('members) as 'member, 'id as 'relationId, 'version, 'timestamp, 'validUntil)
      .withColumn("type", $"member.type")
      .withColumn("id", $"member.ref")
      .drop('member)

    @transient val idByVersion = Window.partitionBy('id).orderBy('version)

    // Create a relation entry for each changeset in which a geometry was modified, containing the timestamp and
    // changeset of the geometry that triggered the association. This will later be used to assemble relations at each
    // of those points in time.
    // If you need authorship, join on changesets
    val relationsByChangeset = geoms
      .where('geometryChanged)
      .drop('validUntil)
      // re-calculate validUntil windows
      .withColumn("validUntil", lead('updated, 1) over idByVersion)
      // TODO when expanding beyond relations referring to ways, geoms should include 'type for the join to work
      // properly
      .withColumn("type", lit(WayType))
      .select('type, 'changeset, 'id, 'updated)
      .join(waysToRelations, Seq("id", "type"))
      .where(waysToRelations("timestamp") <= geoms("updated") and
        geoms("updated") < coalesce(waysToRelations("validUntil"), current_timestamp))
      .select('changeset, 'relationId as 'id, 'version, 'updated)

    @transient val idAndVersionByUpdated = Window.partitionBy('id, 'version).orderBy('updated)
    @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

    val allRelationVersions = relationsByChangeset
      // Union with raw relations to include those in the time line (if they weren't already triggered by geometry
      // modifications at the same time)
      .union(relations.select('changeset, 'id, 'version, 'timestamp as 'updated))
      // If a node, a way, and/or a relation were modified within the same changeset at different times, there will be
      // multiple entries with different timestamps; this reduces them down to a single update per changeset.
      .groupBy('changeset, 'id)
      .agg(max('version).cast(IntegerType) as 'version, max('updated) as 'updated)
      .join(relations.select('id, 'version, 'members), Seq("id", "version"))
      // assign `minorVersion` and rewrite `validUntil` to match
      // this is done early (at the expense of passing through the shuffle w/ exploded 'members) to avoid extremely
      // large partitions (see: Germany w/ 7k+ geometry versions) after geometries have been constructed
      .withColumn("validUntil", lead('updated, 1) over idByUpdated)
      .withColumn("minorVersion", (row_number over idAndVersionByUpdated) - 1)

    allRelationVersions
      .select('changeset, 'id, 'version, 'minorVersion, 'updated, 'validUntil, explode_outer('members) as "member")
      .select(
        'changeset,
        'id,
        'version,
        'minorVersion,
        'updated,
        'validUntil,
        'member.getField("type") as 'type,
        'member.getField("ref") as 'ref,
        'member.getField("role") as 'role
      )
      .distinct
  }

  /**
    * Reconstruct relation geometries.
    *
    * Nodes and ways contain implicit timestamps that will be used to generate minor versions of geometry that they're
    * associated with (each entry that exists within a changeset).
    *
    * @param _relations DataFrame containing relations to reconstruct.
    * @param geoms      DataFrame containing way geometries to use in reconstruction.
    * @return Relations geometries.
    */
  def reconstructRelationGeometries(_relations: DataFrame, geoms: DataFrame)(implicit cache: Caching = Caching.none,
                                                                             cachePartitions: Option[Int] = None)
  : DataFrame = {
    val relations = preprocessRelations(_relations)

    reconstructMultiPolygonRelationGeometries(relations, geoms)
      .union(reconstructRouteRelationGeometries(relations, geoms))
  }

  def reconstructMultiPolygonRelationGeometries(_relations: DataFrame, geoms: DataFrame)(implicit cache: Caching =
  Caching.none,
                                                                                         cachePartitions: Option[Int]
                                                                                         = None)
  : DataFrame = {
    implicit val ss: SparkSession = _relations.sparkSession
    import ss.implicits._
    ss.withJTS

    val relations = preprocessRelations(_relations)
      .where(isMultiPolygon('tags))

    val members = getRelationMembers(relations, geoms)
      .where('role.isin(MultiPolygonRoles: _*))
      // TODO when expanding beyond multipolygons, geoms should include 'type for the join to work properly
      .join(
      geoms.select(
        lit(WayType) as 'type,
        'id as "ref",
        'updated as 'memberUpdated,
        'validUntil as 'memberValidUntil,
        'geom), Seq("type", "ref"), "left_outer")
      .where(
        ('memberUpdated.isNull and 'memberValidUntil.isNull and 'geom.isNull) or // allow left outer join artifacts
          // through
          ('memberUpdated <= 'updated and 'updated < coalesce('memberValidUntil, current_timestamp)))
      .drop('memberUpdated)
      .drop('memberValidUntil)
      .drop('ref)

    implicit val encoder: Encoder[Row] = VersionedElementEncoder

    val relationGeoms = members
        .groupByKey { row =>
          (row.getAs[Long]("changeset"), row.getAs[Long]("id"), row.getAs[Integer]("version"), row.getAs[Integer]
            ("minorVersion"), row.getAs[Timestamp]("updated"), row.getAs[Timestamp]("validUntil"))
        }
        .mapGroups {
          case ((changeset, id, version, minorVersion, updated, validUntil), rows) =>
            val members = rows.toVector
            val types = members.map(_.getAs[Byte]("type"))
            val roles = members.map(_.getAs[String]("role"))
            val geoms = members.map(_.getAs[jts.Geometry]("geom"))

            val wkb = MultiPolygons.build(id, version, updated, types, roles, geoms).orNull

            new GenericRowWithSchema(Array(changeset, id, version, minorVersion, updated, validUntil, wkb),
              VersionedElementSchema): Row
        }

    // Join metadata to avoid passing it through exploded shuffles
    relationGeoms
      .join(relations.select('id, 'version, 'tags, 'visible), Seq("id", "version"))
      .select(
        lit(RelationType) as '_type,
        'id,
        'geom,
        'tags,
        'changeset,
        'updated,
        'validUntil,
        'visible,
        'version,
        'minorVersion)
  }

  def reconstructRouteRelationGeometries(_relations: DataFrame, geoms: DataFrame)(implicit cache: Caching = Caching
    .none,
                                                                                  cachePartitions: Option[Int] = None)
  : DataFrame = {
    implicit val ss: SparkSession = _relations.sparkSession
    import ss.implicits._
    ss.withJTS

    val relations = preprocessRelations(_relations)
      .where(isRoute('tags))

    val members = getRelationMembers(relations, geoms)
      // TODO when expanding beyond way-based routes, geoms should include 'type for the join to work properly
      .join(
      geoms.select(
        lit(WayType) as 'type,
        'id as "ref",
        'updated as 'memberUpdated,
        'validUntil as 'memberValidUntil,
        'geom), Seq("type", "ref"), "left_outer")
      .where(
        ('memberUpdated.isNull and 'memberValidUntil.isNull and 'geom.isNull) or // allow left outer join artifacts
          // through
          ('memberUpdated <= 'updated and 'updated < coalesce('memberValidUntil, current_timestamp)))
      .drop('memberUpdated)
      .drop('memberValidUntil)
      .drop('ref)

    implicit val encoder: Encoder[Row] = TaggedVersionedElementEncoder

    // leverage partitioning (avoids repeated (de-)serialization of merged coordinate arrays)
    val relationGeoms = members
      .groupByKey { row =>
        (row.getAs[Long]("changeset"), row.getAs[Long]("id"), row.getAs[Integer]("version"), row.getAs[Integer]
          ("minorVersion"), row.getAs[Timestamp]("updated"), row.getAs[Timestamp]("validUntil"))
      }
      .flatMapGroups {
        case ((changeset, id, version, minorVersion, updated, validUntil), rows) =>
          val members = rows.toVector
          val types = members.map(_.getAs[Byte]("type"))
          val roles = members.map(_.getAs[String]("role"))
          val geoms = members.map(_.getAs[jts.Geometry]("geom"))

          Routes.build(id, version, updated, types, roles, geoms) match {
            case Some(components) =>
              components.map {
                case ("", wkb) =>
                  // no role
                  new GenericRowWithSchema(Array(changeset, id, Map(), version, minorVersion, updated,
                    validUntil, wkb), TaggedVersionedElementSchema): Row
                case (role, wkb) =>
                  new GenericRowWithSchema(Array(changeset, id, Map("role" -> role), version, minorVersion,
                    updated, validUntil, wkb), TaggedVersionedElementSchema): Row
              }
            case None =>
              // no geometry
              Seq(new GenericRowWithSchema(Array(changeset, id, Map(), version, minorVersion, updated,
                validUntil, null), TaggedVersionedElementSchema): Row)
          }
      }

    // Join metadata to avoid passing it through exploded shuffles
    relationGeoms
      .join(relations.select('id, 'version, 'tags as 'originalTags, 'visible), Seq("id", "version"))
      .select(
        lit(RelationType) as '_type,
        'id,
        'geom,
        mergeTags('originalTags, 'tags) as 'tags,
        'changeset,
        'updated,
        'validUntil,
        'visible,
        'version,
        'minorVersion)
  }

  /**
    * Augment geometries with user metadata.
    *
    * @param geoms      Geometries to augment.
    * @param changesets Changesets DataFrame with user metadata.
    * @return Geometries augmented with user metadata.
    */
  def addUserMetadata(geoms: DataFrame, changesets: DataFrame): DataFrame = {
    import geoms.sparkSession.implicits._

    geoms
      .join(changesets.select('id as 'changeset, 'uid, 'user), Seq("changeset"))
  }

  object Resource {
    def apply(name: String): String = {
      val stream: InputStream = getClass.getResourceAsStream(s"/$name")
      try {
        scala.io.Source.fromInputStream(stream).getLines.mkString(" ")
      } finally {
        stream.close()
      }
    }
  }

  case class CountryId(code: String)

  object MyJsonProtocol extends DefaultJsonProtocol {

    implicit object CountryIdJsonFormat extends RootJsonFormat[CountryId] {
      def read(value: JsValue): CountryId =
        value.asJsObject.getFields("ADM0_A3") match {
          case Seq(JsString(code)) =>
            CountryId(code)
          case v =>
            throw DeserializationException(s"CountryId expected, got $v")
        }

      def write(v: CountryId): JsValue =
        JsObject(
          "code" -> JsString(v.code)
        )
    }

  }

  import MyJsonProtocol._

  object Countries {
    lazy val all: Vector[MultiPolygonFeature[CountryId]] = {
      val collection =
        Resource("countries.geojson").
          parseGeoJson[JsonFeatureCollection]

      val polys =
        collection.
          getAllPolygonFeatures[CountryId].
          map(_.mapGeom(MultiPolygon(_)))

      val mps =
        collection.
          getAllMultiPolygonFeatures[CountryId]

      polys ++ mps
    }

    def indexed: SpatialIndex[MultiPolygonFeature[CountryId]] =
      SpatialIndex.fromExtents(all) { mpf => mpf.geom.envelope }

  }

  class CountryLookup() extends Serializable {
    private val index =
      geotrellis.vector.SpatialIndex.fromExtents(
        Countries.all.
          map { mpf =>
            (mpf.geom.prepare, mpf.data)
          }
      ) { case (pg, _) => pg.geom.envelope }

    def lookup(geom: geotrellis.vector.Geometry): Traversable[CountryId] = {
      val t =
        new Traversable[(geotrellis.vector.prepared.PreparedGeometry[geotrellis.vector.MultiPolygon], CountryId)] {
          override def foreach[U](f: ((geotrellis.vector.prepared.PreparedGeometry[geotrellis.vector.MultiPolygon],
            CountryId)) => U): Unit = {
            val visitor = new org.locationtech.jts.index.ItemVisitor {
              override def visitItem(obj: AnyRef): Unit = f(obj.asInstanceOf[(geotrellis.vector.prepared
              .PreparedGeometry[geotrellis.vector.MultiPolygon], CountryId)])
            }
            index.rtree.query(geom.jtsGeom.getEnvelopeInternal, visitor)
          }
        }

      t.
        filter(_._1.intersects(geom)).
        map(_._2)
    }
  }

  def geocode(geoms: DataFrame): DataFrame = {
    val newSchema = StructType(geoms.schema.fields :+ StructField(
      "countries", ArrayType(StringType, containsNull = false), nullable = true))
    implicit val encoder: Encoder[Row] = RowEncoder(newSchema)

    geoms
      .mapPartitions { partition =>
        val countryLookup = new CountryLookup()

        partition.map { row =>
          val countryCodes = Option(row.getAs[jts.Geometry]("geom")).map(Geometry(_)) match {
            case Some(geom) => countryLookup.lookup(geom).map(x => x.code)
            case None => Seq.empty[String]
          }

          Row.fromSeq(row.toSeq :+ countryCodes)
        }
      }
  }

  def regionsByChangeset(geomCountries: Dataset[Row]): DataFrame = {
    import geomCountries.sparkSession.implicits._

    geomCountries
      .where('country.isNotNull)
      .groupBy('changeset)
      .agg(collect_set('country) as 'countries)

  }

}
