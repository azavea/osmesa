package osmesa

import com.vividsolutions.jts.geom.Coordinate
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import spray.json._

import java.io._

object ProcessOSM {

  def preprocessNodes(history: DataFrame): DataFrame = {
    import history.sparkSession.implicits._

    // Convert BigDecimals to double
    // Reduces size taken for representation at the expense of some precision loss.
    val double = udf((bd: java.math.BigDecimal) => {
      Option(bd).map(_.doubleValue).getOrElse(Double.NaN)
    })

    // Associate last-available tags to deleted nodes and clean coordinates
    @transient val idByUpdated = Window.partitionBy('id).orderBy('version)

    // when a node has been deleted, it doesn't include any tags; use a window function to retrieve the last tags present and use those
    // this is suitable for appending to directly (since none of the values need to change ever)
    val nodes = history
      .where('type === "node")
      .select(
      'id,
      when(!'visible and (lag('tags, 1) over idByUpdated).isNotNull, lag('tags, 1) over idByUpdated).otherwise('tags).as('tags),
      when(!'visible, null).otherwise(double('lat)).as('lat),
      when(!'visible, null).otherwise(double('lon)).as('lon),
      'changeset,
      'timestamp,
      'uid,
      'version,
      'visible)

    // Get the last version of each node modified in a changeset
    // This treats changeset closure as "intended state" by the editor.
    val nodeVersions = nodes
      .groupBy('id, 'changeset)
      .agg(max('version).as('version))
      .drop('changeset)

    // Add `validUntil`.  This allows time slices to be made more effectively by filtering for nodes that were valid between `timestamp`
    // and `validUntil`.  Nodes with `null` `validUntil` are currently valid.
    // Select full metadata for the last version of a node by changeset and add "validUntil" for joining with other types
    // this means that the final edit within a changeset is the intended end state for a given node (after possibly having been moved
    // around or adjusted to deal with edits in other changesets)
    val ns = nodes
      .join(nodeVersions, Seq("id", "version"))
      .withColumn("validUntil", lead('timestamp, 1) over idByUpdated)

    // Write out pre-processed nodes
    // snapshot of historical nodes with validity ranges
    // ns.repartition(1).write.format("orc").save("data/ri-nodes")

    // Create a "planet" equivalent for currently-valid nodes
    // "planet" snapshot nodes
    // ns.where('validUntil.isNull and 'visible)
    //   .drop('validUntil)
    //   .drop('visible)

    ns
  }

  def preprocessWays(history: DataFrame): DataFrame = {
    import history.sparkSession.implicits._

    // Prepare ways for geometric assembly.
    // Associate last-available tags to deleted ways

    @transient val idByUpdated = Window.partitionBy('id).orderBy('version)

    // when an element has been deleted, it doesn't include any tags; use a window function to retrieve the last tags present and use those
    // this is suitable for appending to directly (since none of the values need to change ever)
    val ways = history
      .where('type === "way")
      .select(
        'id,
        when(!'visible and (lag('tags, 1) over idByUpdated).isNotNull, lag('tags, 1) over idByUpdated).otherwise('tags).as('tags),
        $"nds.ref".as('nds),
        'changeset,
        'timestamp,
        'uid,
        'version,
        'visible
      )

    // Get the last version of each way modified in a changeset
    // This treats changeset closure as "intended state" by the editor.
    val wayVersions = ways
      .groupBy('id, 'changeset)
      .agg(max('version).as('version))
      .drop('changeset)

    // Add `validUntil`
    // This allows time slices to be made more effectively by filtering for ways that were valid between `timestamp` and `validUntil`.
    // Ways with `null` `validUntil` are currently valid.
    // Select full metadata for the last version of a way by changeset and add "validUntil" for joining with other types.
    // This means that the final edit within a changeset is the intended end state for a given way (after possibly having been moved
    // around or adjusted to deal with edits in other changesets)
    val ws = ways
      .join(wayVersions, Seq("id", "version"))
      .withColumn("validUntil", lead('timestamp, 1) over idByUpdated)

    // Write out pre-processed ways
    // snapshot of historical ways with validity ranges
    // ws.repartition(1).write.format("orc").save("data/ri-ways")

    // Create a “planet” equivalent for currently-valid ways
    // "planet" snapshot ways
    // NOTE: nds is array<long> rather than array<struct<ref:long>> (as in the PDS ORC files)
    // ws.where('validUntil.isNull and 'visible)
    //   .drop('validUntil)
    //   .drop('visible)

    ws
  }

  def constructPointGeometries(nodes: DataFrame): DataFrame = {
    import nodes.sparkSession.implicits._

    // Point generation
    val asWKB = udf((x: Double, y: Double) =>
      (x, y) match {
        // drop ways with invalid coordinates
        case (x, y) if x.equals(null) || y.equals(null) || x.equals(Double.NaN) || y.equals(Double.NaN) => null
        // drop ways that don't contain valid geometries
        case (x, y) => Point(x, y).toWKB(4326)
      }
    )

    val nodeCreations = nodes
      .groupBy('id)
      .agg(min('version))
      .join(nodes, Seq("id"), "inner")
      .select('id, 'timestamp.as('creation_time))

    // Create point geometries
    // "Uninteresting" (untagged) nodes are not created as points, as creation is simple enough that this can be done when
    // assembling way and relation geometries.
    nodes
      .where(size('tags) > 0)
      .select('changeset, 'id, 'version, 'tags, asWKB('lon, 'lat).as('geom), 'timestamp.as('updated), 'validUntil, 'visible)
      .where('visible and isnull('validUntil)) // This filters things down to all and only the most current geoms which are visible
      .where('visible and isnull('validUntil)) // This filters things down to all and only the most current geoms which are visible
      .join(nodeCreations, Seq("id"), "inner")

  }

  def reconstructWayGeometries(ppnodes: DataFrame, ppways: DataFrame): DataFrame = {
    import ppnodes.sparkSession.implicits._

    // Load ways and nodes
    // These should have already been pre-processed to incorporate a `validUntil` column.
    // some nodes at (0, 0) are valid, but most are not (and some are redacted, which causes problems when clipping the
    // resulting geometries to a grid)
    val nodes = ppnodes.where('lat =!= 0 and 'lon =!= 0)
    val ways = ppways

    // Create a lookup table for node → ways
    val nodesToWays = ways
      .select(explode('nds).as('id), 'id.as('way_id), 'version, 'timestamp, 'validUntil)

    // to facilitate providing a list of source timestamps
    val nodesInChangeset = nodes
    val waysInChangeset = ways

    // Create a way entry for each changeset in which a node was modified, containing the timestamp of the node that triggered
    // the association. This will later be used to assemble ways at each of those points in time.
    val referencedWaysByChangeset = nodesInChangeset
      .select('changeset, 'id, 'timestamp.as('updated))
      .join(nodesToWays, Seq("id"))
      .where('timestamp <= 'updated and 'updated < coalesce('validUntil, current_timestamp))
      .select('changeset, 'way_id.as('id), 'version, 'updated)
      .groupBy('changeset, 'id)
      .agg(max('version).as('version), max('updated).as('updated))

    // join w/ referencedWaysByChangeset (on changeset, way_id) later to pick up way versions
    // Union with raw ways to include those in the timeline (if they weren't already triggered by node modifications at the same time)
    // If a node and a way were modified within the same changeset at different times, there will be 2 entries (where there should
    // probably be one, grouped by the changeset).
    val allReferencedWays = ways.select('id, 'version, 'nds, 'visible)
      .join(referencedWaysByChangeset, Seq("id", "version"))
      .union(waysInChangeset.select('id, 'version, 'nds, 'visible, 'changeset, 'timestamp.as('updated)))
      .distinct

    val fullReferencedWays = allReferencedWays
      .select('changeset, 'id, 'version, 'updated, 'visible, posexplode_outer('nds).as(Seq("idx", "ref")))
      .repartition('id, 'updated) // repartition including updated timestamp to avoid skew (version is insufficient, as
                                  // multiple instances may exist with the same version)

    val joinedWays = fullReferencedWays
      .join(nodes.select('id.as('ref), 'version.as('ref_version), 'timestamp, 'validUntil), Seq("ref"), "left_outer")
      .where(!'visible or 'timestamp <= 'updated and 'updated < coalesce('validUntil, current_timestamp))

    val fullJoinedWays = joinedWays
      .join(nodes.select('id.as('ref), 'version.as('ref_version), 'lat, 'lon), Seq("ref", "ref_version"), "left_outer")
      .select('changeset, 'id, 'version, 'updated, 'idx, 'lat, 'lon, 'visible)
      .groupBy('changeset, 'id, 'version, 'updated, 'visible)
      .agg(collect_list(struct('idx, 'lon, 'lat)).as('coords))

    // Join tags into ways w/ nodes
    val taggedWays = fullJoinedWays
      .join(ways.select('id, 'version, 'tags), Seq("id", "version"))

    val _isArea = (tags: Map[String, String]) =>
    tags match {
      // TODO yes, true, 1
      case tags if tags.contains("area") && Seq("yes", "no").contains(tags("area").toLowerCase) => tags("area").toLowerCase == "yes"
      case tags =>
        // see https://github.com/osmlab/id-area-keys (values are inverted)
        val matchingKeys = tags.keySet.intersect(Constants.AREA_KEYS.keySet)
        matchingKeys.exists(k => !Constants.AREA_KEYS(k).contains(tags(k)))
    }

    val isArea = udf(_isArea)

    // Define geometry-related UDFs

    val asWKB = udf((ways: Seq[Row], isArea: Boolean) => {
      val coords = ways
        .sortWith(_.getAs[Int]("idx") < _.getAs[Int]("idx"))
        .map(row => (row.getAs[Double]("lon"), row.getAs[Double]("lat")))

      val geom = coords match {
        // drop ways with invalid coordinates
        // check for nulls (or add a coalesce to the select after join); since posexplode_outer is used, there will always be
        // a row but it may contain null (not NaN) values
        case _ if coords.exists { case (x, y) => x == null || x.equals(Double.NaN) || y == null || y.equals(Double.NaN) } => None
        // drop ways that don't contain valid geometries
        case _ if coords.isEmpty => None
        case _ if coords.length == 1 =>
          Some(Point(coords.head._1, coords.head._2))
        case _ =>
          val line = Line(coords)

          Some(isArea match {
            case true if line.isClosed && coords.length >= 4 => Polygon(line)
            case _ => line
          })
      }

      geom match {
        // drop invalid geometries
        case Some(g) if g.isValid => g.toWKB(4326)
        case _ => null
      }
    })

    // useful for debugging
    val isValid = udf((geom: Array[Byte]) => {
      geom match {
        case null => true
        case _ => geom.readWKB.isValid
      }
    })

    // useful for debugging; some geometries that are valid as 4326 are not as 3857
    val reproject = udf((geom: Array[Byte]) => {
      geom match {
        case null => null
        case _ => geom.readWKB.reproject(LatLng, WebMercator).toWKB(3857)
      }
    })

    // useful for debugging
    val asWKT = udf((geom: Array[Byte]) => {
      geom match {
        case null => ""
        case _ => geom.readWKB.toWKT
      }
    })

    // Create WKB geometries (LineStrings and Polygons)
    val wayGeoms = taggedWays
      .withColumn("geom", asWKB('coords, isArea('tags)))
      .select('changeset, 'id, 'version, 'tags, 'geom, 'updated, 'visible)

    // Assign `minorVersion` and rewrite `validUntil` to match
    @transient val idAndVersionByUpdated = Window.partitionBy('id, 'version).orderBy('updated)
    @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

    val wayCreations = ppways
      .groupBy('id)
      .agg(min('version))
      .join(ppways, Seq("id"), "inner")
      .select('id, 'timestamp.as('creation_time))

    wayGeoms
      .withColumn("validUntil", lead('updated, 1) over idByUpdated)
      .withColumn("minorVersion", (row_number() over idAndVersionByUpdated) - 1)
      .select('changeset, 'id, 'version, 'tags, 'geom, 'updated, 'validUntil, 'visible, 'minorVersion)
      .where('visible and isnull('validUntil)) // This filters things down to all and only the most current geoms which are visible
      .join(wayCreations, Seq("id"), "inner")
  }

  def geometriesByRegion(nodeGeoms: Dataset[Row], wayGeoms: Dataset[Row]): DataFrame = {
    import nodeGeoms.sparkSession.implicits._

    // Geocode geometries by country
    val geoms = nodeGeoms.union(wayGeoms)

    object Resource {
      def apply(name: String): String = {
        val stream: InputStream = getClass.getResourceAsStream(s"/$name")
        try { scala.io.Source.fromInputStream( stream ).getLines.mkString(" ") } finally { stream.close() }
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
              throw new DeserializationException(s"CountryId expected, got $v")
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
    val geomCountries = geoms
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
    .cache

    geomCountries
      .where('country.isNotNull)
      .groupBy('changeset)
      .agg(collect_set('country).as('countries))

  }

}
