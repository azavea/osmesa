package osmesa

import java.io._
import java.sql.Timestamp

import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json._
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import osmesa.functions._
import osmesa.functions.osm._
import spray.json._

object ProcessOSM {
  val NODE_TYPE: Byte = 1
  val WAY_TYPE: Byte = 2
  val RELATION_TYPE: Byte = 3

  lazy val logger: Logger = Logger.getRootLogger

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
        'timestamp <= coalesce(lit(timestamp), current_timestamp)
          and coalesce(lit(timestamp), current_timestamp) < coalesce('validUntil, date_add(current_timestamp, 1)))
  }

  /**
    * Pre-process nodes. Copies coordinates + tags from versions prior to being deleted (!'visible), as they're cleared
    * out otherwise, and adds 'validUntil.
    *
    * @param history DataFrame containing nodes.
    * @return processed nodes.
    */
  def preprocessNodes(history: DataFrame): DataFrame = {
    import history.sparkSession.implicits._

    if (history.columns.contains("validUntil")) {
      history
    } else {
      @transient val idByUpdated = Window.partitionBy('id).orderBy('version)

      // when a node has been deleted, it doesn't include any tags; use a window function to retrieve the last tags present and use those
      // this is suitable for appending to directly (since none of the values need to change ever)

      // Add `validUntil`.  This allows time slices to be made more effectively by filtering for nodes that were valid between `timestamp`
      // and `validUntil`.  Nodes with `null` `validUntil` are currently valid.
      history
        .where('type === "node")
        .select(
          'id,
          when(!'visible and (lag('tags, 1) over idByUpdated).isNotNull, lag('tags, 1) over idByUpdated).otherwise('tags) as 'tags,
          when(!'visible, null).otherwise(asDouble('lat)) as 'lat,
          when(!'visible, null).otherwise(asDouble('lon)) as 'lon,
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
    * Pre-process ways. Copies tags from versions prior to being deleted (!'visible), as they're cleared out
    * otherwise, dereferences 'nds.ref, and adds 'validUntil.
    *
    * @param history DataFrame containing ways.
    * @return processed ways.
    */
  def preprocessWays(history: DataFrame): DataFrame = {
    import history.sparkSession.implicits._

    if (history.columns.contains("validUntil")) {
      history
    } else {
      @transient val idByUpdated = Window.partitionBy('id).orderBy('version)

      // when a node has been deleted, it doesn't include any tags; use a window function to retrieve the last tags present and use those
      // this is suitable for appending to directly (since none of the values need to change ever)

      // Add `validUntil`.  This allows time slices to be made more effectively by filtering for nodes that were valid between `timestamp`
      // and `validUntil`.  Nodes with `null` `validUntil` are currently valid.
      history
        .where('type === "way")
        .select(
          'id,
          when(!'visible and (lag('tags, 1) over idByUpdated).isNotNull, lag('tags, 1) over idByUpdated).otherwise('tags) as 'tags,
          $"nds.ref" as 'nds,
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
    * Pre-process relations. Copies tags from versions prior to being deleted (!'visible), as they're cleared out
    * otherwise, and adds 'validUntil.
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

      // when a node has been deleted, it doesn't include any tags; use a window function to retrieve the last tags present and use those
      // this is suitable for appending to directly (since none of the values need to change ever)

      // Add `validUntil`.  This allows time slices to be made more effectively by filtering for nodes that were valid between `timestamp`
      // and `validUntil`.  Nodes with `null` `validUntil` are currently valid.
      history
        .where('type === "relation")
        .repartition('id)
        .select(
          'id,
          when(!'visible and (lag('tags, 1) over idByUpdated).isNotNull, lag('tags, 1) over idByUpdated).otherwise('tags) as 'tags,
          'members,
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

    val nodes = ProcessOSM.preprocessNodes(elements)

    val nodeGeoms = ProcessOSM.constructPointGeometries(nodes)
      .withColumn("minorVersion", lit(0))

    val wayGeoms = ProcessOSM.reconstructWayGeometries(elements, nodes)

    val relationGeoms = ProcessOSM.reconstructRelationGeometries(elements, wayGeoms)

    // TODO remove way geoms that contribute to relations but have no inherent value (unclear how to identify these)
    nodeGeoms
      .union(wayGeoms.where(size('tags) > 0))
      .union(relationGeoms)
  }

  /**
    * Construct point geometries. "Uninteresting" nodes are not included (although they may be necessary for way +
    * relation assembly).
    *
    * @param nodes DataFrame containing nodes
    * @return Nodes as Point geometries
    */
  def constructPointGeometries(nodes: DataFrame): DataFrame = {
    import nodes.sparkSession.implicits._

    preprocessNodes(nodes)
      .where(size('tags) > 0)
      .select(
        lit(NODE_TYPE) as '_type,
        'id,
        ST_Point('lon, 'lat) as 'geom,
        'tags,
        'changeset,
        'timestamp as 'updated,
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
  def reconstructWayGeometries(_ways: DataFrame, _nodes: DataFrame, _nodesToWays: Option[DataFrame] = None): DataFrame = {
    import _nodes.sparkSession.implicits._

    val nodes = preprocessNodes(_nodes)
      // some nodes at (0, 0) are valid, but most are not (and some are redacted, which causes problems when clipping the
      // resulting geometries to a grid)
      // TODO this probably needs to be fixed in osm2orc by writing out Double.NaN instead of null (which requires
      // osm4j-core to not use primitive types for node coordinates)
      .where('lat =!= 0 and 'lon =!= 0)

    val ways = preprocessWays(_ways)
      .withColumn("isArea", isArea('tags))

    // Create (or re-use) a lookup table for node → ways
    val nodesToWays = _nodesToWays.getOrElse[DataFrame] {
      ways
        .select(explode('nds) as 'id, 'id as 'wayId, 'version, 'timestamp, 'validUntil)
    }

    // Create a way entry for each changeset in which a node was modified, containing the timestamp of the node that triggered
    // the association. This will later be used to assemble ways at each of those points in time.
    // If you need authorship, join on changesets
    val waysByChangeset = nodes
      .select('changeset, 'id, 'timestamp as 'updated)
      .join(nodesToWays, Seq("id"))
      .where('timestamp <= 'updated and 'updated < coalesce('validUntil, current_timestamp))
      .select('changeset, 'wayId as 'id, 'version, 'updated)
      .groupBy('changeset, 'id)
      .agg(max('version) as 'version, max('updated) as 'updated)

    // join w/ waysByChangeset (on changeset, way id) later to pick up way versions
    // Union with raw ways to include those in the time line (if they weren't already triggered by node modifications at the same time)
    // If a node and a way were modified within the same changeset at different times, there will be 2 entries (where there should
    // probably be one, grouped by the changeset).
    val allWayVersions = ways.select('id, 'version, 'nds, 'isArea)
      .join(waysByChangeset, Seq("id", "version"))
      .union(ways.select('id, 'version, 'nds, 'isArea, 'changeset, 'timestamp as 'update))
      .distinct

    val explodedWays = allWayVersions
      .select('changeset, 'id, 'version, 'updated, 'isArea, posexplode_outer('nds) as Seq("idx", "ref"))
      // repartition including updated timestamp to avoid skew (version is insufficient, as
      // multiple instances may exist with the same version)
      .repartition('id, 'updated)

    val waysAndNodes = explodedWays
      .join(nodes.select('id as 'ref, 'version as 'ref_version, 'timestamp, 'validUntil), Seq("ref"), "left_outer")
      .where('timestamp <= 'updated and 'updated < coalesce('validUntil, current_timestamp))

    val wayGeoms = waysAndNodes
      .join(nodes.select('id as 'ref, 'version as 'ref_version, 'lat, 'lon), Seq("ref", "ref_version"), "left_outer")
      .select('changeset, 'id, 'version, 'updated, 'isArea, 'idx, 'lat, 'lon)
      .groupBy('changeset, 'id, 'version, 'updated)
      // Create WKB geometries (LineStrings and Polygons)
      .agg(collectWay('isArea, 'idx, 'lon, 'lat) as 'geom)

    // Assign `minorVersion` and rewrite `validUntil` to match
    @transient val idAndVersionByUpdated = Window.partitionBy('id, 'version).orderBy('updated)
    @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

    wayGeoms
      .join(ways.select('id, 'version, 'tags, 'visible), Seq("id", "version"))
      .select(
        lit(WAY_TYPE) as '_type,
        'id,
        'geom,
        'tags,
        'changeset,
        'updated,
        (lead('updated, 1) over idByUpdated) as 'validUntil,
        'visible,
        'version,
        ((row_number over idAndVersionByUpdated) - 1) as 'minorVersion)
  }

  /**
    * Reconstruct relation geometries.
    *
    * Nodes and ways contain implicit timestamps that will be used to generate minor versions of geometry that they're
    * associated with (each entry that exists within a changeset).
    *
    * @param _relations DataFrame containing relations to reconstruct.
    * @param geoms     DataFrame containing way geometries to use in reconstruction.
    * @return Relations geometries.
    */
  def reconstructRelationGeometries(_relations: DataFrame, geoms: DataFrame): DataFrame = {
    import _relations.sparkSession.implicits._

    // TODO 1280388@v1 for an old-style multipolygon (tags on ways)

    val relations = preprocessRelations(_relations).where(isMultiPolygon('tags))

    val waysToRelations = relations
      .select(explode('members) as 'member, 'id as 'relationId, 'version, 'timestamp, 'validUntil)
      .withColumn("type", $"member.type")
      .withColumn("id", $"member.ref")
      // role isn't needed for any lookups (here)
      // .withColumn("role", $"member.role")
      .drop('member)

    // Create a relation entry for each changeset in which a geometry was modified, containing the timestamp and
    // changeset of the geometry that triggered the association. This will later be used to assemble relations at each
    // of those points in time.
    // If you need authorship, join on changesets
    val relationsByChangeset = geoms
      // TODO when expanding beyond multipolygons, geoms should include 'type for the join to work properly
      .withColumn("type", lit("way"))
      .select('type, 'changeset, 'id, 'updated)
      .join(waysToRelations, Seq("id", "type"))
      .where(waysToRelations("timestamp") <= geoms("updated") and geoms("updated") < coalesce(waysToRelations("validUntil"), current_timestamp))
      .select('changeset, 'relationId as 'id, 'version, 'updated)
      .groupBy('changeset, 'id)
      .agg(max('version) as 'version, max('updated) as 'updated)

    // If a node and a way were modified within the same changeset at different times, there will be 2 entries (where
    // there should probably be one, grouped by the changeset).
    val allRelationVersions = relations.select('id, 'version, 'members, 'visible)
      // join w/ relationsByChangeset (on changeset, relation id) later to pick up relation versions
      .join(relationsByChangeset, Seq("id", "version"))
      // Union with raw relations to include those in the time line (if they weren't already triggered by geometry
      // modifications at the same time)
      .union(relations.select('id, 'version, 'members, 'visible, 'changeset, 'timestamp as 'updated))
      .distinct

    val members = allRelationVersions
      .select('changeset, 'id, 'version, 'updated, explode_outer('members) as "member")
      .select(
        'changeset,
        'id,
        'version,
        'updated,
        'member.getField("type") as 'type,
        'member.getField("ref") as 'ref,
        'member.getField("role") as 'role
      )
      .repartition('changeset, 'id, 'version, 'updated)
      // TODO when expanding beyond multipolygons, geoms should include 'type for the join to work properly
      .join(geoms.select(lit("way") as 'type, 'id as "ref", 'updated, 'validUntil, 'geom), Seq("type", "ref"), "left_outer")
      .where(
        'geom.isNull or // allow null geoms through so we can check data validity later
          (geoms("updated") <= allRelationVersions("updated") and allRelationVersions("updated") < coalesce(geoms("validUntil"), current_timestamp)))
      .drop(geoms("updated"))
      .drop(geoms("validUntil"))

    val relationGeoms = members
      .groupBy('changeset, 'id, 'version, 'updated)
      .agg(collectRelation('id, 'version, 'updated, 'type, 'role, 'geom) as 'geom)

    // Assign `minorVersion` and rewrite `validUntil` to match
    @transient val idAndVersionByUpdated = Window.partitionBy('id, 'version).orderBy('updated)
    @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

    // Join metadata to avoid passing it through exploded shuffles
    relationGeoms
      .join(relations.select('id, 'version, 'tags, 'visible), Seq("id", "version"))
      .select(
        lit(RELATION_TYPE) as '_type,
        'id,
        'geom,
        'tags,
        'changeset,
        'updated,
        (lead('updated, 1) over idByUpdated) as 'validUntil,
        'visible,
        'version,
        ((row_number over idAndVersionByUpdated) - 1) as 'minorVersion)
  }

  /**
    * Augment geometries with user metadata.
    *
    * @param geoms Geometries to augment.
    * @param changesets Changesets DataFrame with user metadata.
    * @return Geometries augmented with user metadata.
    */
  def addUserMetadata(geoms: DataFrame, changesets: DataFrame): DataFrame = {
    import geoms.sparkSession.implicits._

    geoms
      .join(changesets.select('id as 'changeset, 'uid, 'user), Seq("changeset"))
  }

  def geometriesByRegion(nodeGeoms: DataFrame, wayGeoms: DataFrame): DataFrame = {
    import nodeGeoms.sparkSession.implicits._

    // Geocode geometries by country
    val geoms = nodeGeoms
      .withColumn("minorVersion", lit(0))
      .withColumn("type", lit("node"))
      .where('geom.isNotNull and size('tags) > 0)
      .union(wayGeoms
        .withColumn("type", lit("way"))
        .where('geom.isNotNull and size('tags) > 0)
      )
      .repartition('id, 'type, 'updated)

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
      def all: Vector[MultiPolygonFeature[CountryId]] = {
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
            override def foreach[U](f: ((geotrellis.vector.prepared.PreparedGeometry[geotrellis.vector.MultiPolygon], CountryId)) => U): Unit = {
              val visitor = new com.vividsolutions.jts.index.ItemVisitor {
                override def visitItem(obj: AnyRef): Unit = f(obj.asInstanceOf[(geotrellis.vector.prepared.PreparedGeometry[geotrellis.vector.MultiPolygon], CountryId)])
              }
              index.rtree.query(geom.jtsGeom.getEnvelopeInternal, visitor)
            }
          }

        t.
          filter(_._1.intersects(geom)).
          map(_._2)
      }
    }
    geoms
      .mapPartitions { partition =>
        val countryLookup = new CountryLookup()

        partition.flatMap { row =>
          val changeset = row.getAs[Long]("changeset")
          val t = row.getAs[String]("type")
          val id = row.getAs[Long]("id")
          val geom = row.getAs[scala.Array[Byte]]("geom")

          countryLookup.lookup(geom.readWKB).map(x => (changeset, t, id, x.code))
        }
      }
      .toDF("changeset", "type", "id", "country")
  }

  def regionsByChangeset(geomCountries: Dataset[Row]): DataFrame = {
    import geomCountries.sparkSession.implicits._

    geomCountries
      .where('country.isNotNull)
      .groupBy('changeset)
      .agg(collect_set('country) as 'countries)

  }

}
