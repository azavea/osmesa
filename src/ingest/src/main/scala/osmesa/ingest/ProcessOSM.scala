package osmesa

import java.io._
import java.sql.Timestamp

import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json._
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{udf, _}
import osmesa.udfs._
import spray.json._

import scala.annotation.tailrec

object ProcessOSM {

  lazy val logger = Logger.getRootLogger

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
      'user,
      'version,
      'visible)

    // Get the last version of each node modified in a changeset
    // This treats changeset closure as "intended state" by the editor.
    val nodeVersions = nodes
      .groupBy('id, 'changeset)
      .agg(max('version).as('version))
      .drop('changeset)

    // Creation times for nodes
    val nodeCreations = nodes
      .groupBy('id)
      .agg(min('timestamp).as('creation), collect_set('user).as('authors))

    // Add `validUntil`.  This allows time slices to be made more effectively by filtering for nodes that were valid between `timestamp`
    // and `validUntil`.  Nodes with `null` `validUntil` are currently valid.
    // Select full metadata for the last version of a node by changeset and add "validUntil" for joining with other types
    // this means that the final edit within a changeset is the intended end state for a given node (after possibly having been moved
    // around or adjusted to deal with edits in other changesets)
    val ns = nodes
      .join(nodeVersions, Seq("id", "version"))
      .join(nodeCreations, Seq("id"))
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
        'user,
        'version,
        'visible
      )

    // Get the last version of each way modified in a changeset
    // This treats changeset closure as "intended state" by the editor.
    val wayVersions = ways
      .groupBy('id, 'changeset)
      .agg(max('version).as('version))
      .drop('changeset)

    // Creation times for ways
    val wayCreations = ways
      .groupBy('id)
      .agg(min('timestamp).as('creation), collect_set('user).as('authors))

    // Add `validUntil`
    // This allows time slices to be made more effectively by filtering for ways that were valid between `timestamp` and `validUntil`.
    // Ways with `null` `validUntil` are currently valid.
    // Select full metadata for the last version of a way by changeset and add "validUntil" for joining with other types.
    // This means that the final edit within a changeset is the intended end state for a given way (after possibly having been moved
    // around or adjusted to deal with edits in other changesets)
    val ws = ways
      .join(wayVersions, Seq("id", "version"))
      .join(wayCreations, Seq("id"))
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

  def preprocessRelations(history: DataFrame): DataFrame = {
    import history.sparkSession.implicits._

    // Prepare relations for geometric assembly.
    // Associate last-available tags to deleted ways

    @transient val idByUpdated = Window.partitionBy('id).orderBy('version)

    // when an element has been deleted, it doesn't include any tags; use a window function to retrieve the last tags present and use those
    // this is suitable for appending to directly (since none of the values need to change ever)
    val relations = history
      .where('type === "relation")
      .select(
        'id,
        when(!'visible and (lag('tags, 1) over idByUpdated).isNotNull, lag('tags, 1) over idByUpdated).otherwise('tags).as('tags),
        'members,
        'changeset,
        'timestamp,
        'uid,
        'user,
        'version,
        'visible)

    // get the last version of each relation by changeset
    val relationVersions = relations
      .groupBy('id, 'changeset)
      .agg(max('version).as('version))
      .drop('changeset)

    // Creation times for relations
    val relationCreations = relations
      .groupBy('id)
      .agg(min('timestamp).as('creation), collect_set('user).as('authors))

    // Add `validUntil`
    // This allows time slices to be made more effectively by filtering for relations that were valid between `timestamp` and `validUntil`.
    // select full metadata for the last version of a relation by changeset and add "validUntil" for joining with other types
    // this means that the final edit within a changeset is the intended end state for a given relation (after possibly having been moved around or adjusted to deal with edits in other changesets)
    val rs = relations
      .join(relationVersions, Seq("id", "version"))
      .withColumn("validUntil", lead('timestamp, 1) over idByUpdated)

    // Write out pre-processed relations
    // snapshot of historical relations with validity ranges
    // rs.repartition(1).write.format("orc").save("data/ri-relations")

    // Create a “planet” equivalent for currently-valid relations
    // "planet" snapshot relation
    // rs
    //   .where('validUntil.isNull and 'visible)
    //   .drop('validUntil)
    //   .drop('visible)

    rs
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

    // Create point geometries
    // "Uninteresting" (untagged) nodes are not created as points, as creation is simple enough that this can be done when
    // assembling way and relation geometries.
    nodes
      .where(size('tags) > 0)
      .select('changeset, 'id, 'version, 'tags, asWKB('lon, 'lat).as('geom), 'timestamp.as('updated), 'validUntil, 'visible, 'creation, 'authors, 'user.as('lastAuthor))
      .where('visible and isnull('validUntil)) // This filters things down to all and only the most current geoms which are visible

  }

  /**
    * Reconstruct way geometries.
    *
    * Nodes and ways contain implicit timestamps that will be used to generate minor versions of geometry that they're
    * associated with (each entry that exists within a changeset).
    *
    * @param _nodes
    * @param _ways
    * @param _nodesToWays
    * @return
    */
  def reconstructWayGeometries(_nodes: DataFrame, _ways: DataFrame, _nodesToWays: Option[DataFrame] = None): DataFrame = {
    import _nodes.sparkSession.implicits._

    // These should have already been pre-processed to incorporate a `validUntil` column; if not, do so
    // TODO move this check into preprocessNodes
    val nodes = if (_nodes.columns.contains("validUntil")) {
      _nodes
    } else {
      preprocessNodes(_nodes)
    }
      // some nodes at (0, 0) are valid, but most are not (and some are redacted, which causes problems when clipping the
      // resulting geometries to a grid)
      .where('lat =!= 0 and 'lon =!= 0)

    // TODO move this check into preprocessWays
    val ways = if (_ways.columns.contains("validUntil")) {
      _ways
    } else {
      preprocessWays(_ways)
    }

    // Create (or re-use) a lookup table for node → ways
    val nodesToWays = _nodesToWays.getOrElse[DataFrame] {
      ways
        .select(explode('nds).as('id), 'id.as('way_id), 'version, 'timestamp, 'validUntil)
    }

    // Create a way entry for each changeset in which a node was modified, containing the timestamp of the node that triggered
    // the association. This will later be used to assemble ways at each of those points in time.
    val referencedWaysByChangeset = nodes
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
      .union(ways.select('id, 'version, 'nds, 'visible, 'changeset, 'timestamp.as('updated)))
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

    // TODO extract into udf package
    val _isArea = (tags: Map[String, String]) =>
      tags match {
        case tags if tags.contains("area") && Set("yes", "no", "true", "1").contains(tags("area").toLowerCase) =>
          Set("yes", "true", "1").contains(tags("area").toLowerCase)
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

    // Create WKB geometries (LineStrings and Polygons)
    val wayGeoms = taggedWays
      .withColumn("geom", asWKB('coords, isArea('tags)))
      .select('changeset, 'id, 'version, 'tags, 'geom, 'updated, 'visible)

    // TODO extract into an augmentWays function
    //    val augmentedFields = ways.select('id, 'version, 'creation, 'authors, 'user.as('lastAuthor))

    // Assign `minorVersion` and rewrite `validUntil` to match
    @transient val idAndVersionByUpdated = Window.partitionBy('id, 'version).orderBy('updated)
    @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

    wayGeoms
      .withColumn("validUntil", lead('updated, 1) over idByUpdated)
      .withColumn("minorVersion", (row_number() over idAndVersionByUpdated) - 1)
      .select('changeset, 'id, 'version, 'tags, 'geom, 'updated, 'validUntil, 'visible, 'minorVersion)
    //      .join(augmentedFields, Seq("id", "version")) // TODO extract into augmentWays function
    //      .where('visible and isnull('validUntil)) // This filters things down to all and only the most current geoms which are visible TODO extract into a latest function
  }

  //  def reconstructRelationGeometries(ppnodes: DataFrame, wayGeoms: DataFrame, pprelations: DataFrame): DataFrame = ???

  // create fully-formed rings from line segments
  @tailrec
  private def connectSegments(segments: List[Line], rings: List[Line] = List.empty[Line]): List[Line] = {
    segments match {
      case Nil => rings
      case h :: t if h.isClosed => connectSegments(t, rings :+ h)
      case h :: t =>
        connectSegments(t.find(line => h.vertices.last == line.vertices.head) match {
          case Some(next) => Line(h.vertices ++ next.vertices.tail) :: t.filterNot(line => line == next)
          case None =>
            t.find(line => h.vertices.last == line.vertices.last) match {
              case Some(next) => Line(h.vertices ++ next.vertices.reverse.tail) :: t.filterNot(line => line == next)
              case None => throw new Exception("Unable to connect segments.")
            }
        }, rings)
    }
  }

  @tailrec
  private def dissolveRings(rings: List[Polygon], dissolvedOuters: List[Line] = List.empty[Line], dissolvedInners: List[Line] = List.empty[Line]): (List[Line], List[Line]) = {
    rings match {
      case Nil => (dissolvedOuters, dissolvedInners)
      case h :: t =>
        t.filter(r => h.touches(r)) match {
          case touching if touching.isEmpty => dissolveRings(t.filterNot(r => h.touches(r)), dissolvedOuters :+ h.exterior, dissolvedInners ++ h.holes)
          case touching =>
            val dissolved = touching.foldLeft(List(h)) {
              case (rs, r2) =>
                rs.flatMap { r =>
                  r.union(r2).toGeometry match {
                    case Some(p: Polygon) => List(p)
                    case Some(mp: MultiPolygon) => mp.polygons
                    case _ => throw new Exception("Union failed.")
                  }
                }
            }

            val remaining = t.filterNot(r => h.touches(r))
            val retryRings = dissolved.filter(d => remaining.exists(r => r.touches(d)))
            val newRings = dissolved.filter(d => !remaining.exists(r => r.touches(d)))

            dissolveRings(retryRings ++ remaining, dissolvedOuters ++ newRings.map(_.exterior), dissolvedInners ++ newRings.flatMap(_.holes))
        }
    }
  }

  private def dissolveRings(rings: List[Line]): (List[Line], List[Line]) = {
    dissolveRings(rings.map(x => Polygon(x)))
  }

  // TODO type=route relations
  // TODO filter relations referring to other relations
  // TODO remove ways without unique tags that participate in multipolygon relations

  val buildMultiPolygon: UserDefinedFunction = udf((ways: Seq[Row], id: Long, version: Long, timestamp: Timestamp) => {
    try {
      // bail early if null values are present where they should exist (members w/ type=way)
      if (ways.exists(row => row.getAs[String]("type") == "way" && row.getAs[Array[Byte]]("geom") == null)) {
        logger.debug(s"Incomplete relation: $id @ $version ($timestamp)")
        null
      } else {
        val coords: Seq[(String, Line)] = ways
          .map(row => (row.getAs[String]("role"), Option(row.getAs[Array[Byte]]("geom")).map(_.readWKB) match {
            case Some(geom: Polygon) => geom.as[Polygon].map(_.exterior)
            case Some(geom) => geom.as[Line]
            case None => None
          })).filter(_._2.isDefined).map(x => (x._1, x._2.get))

        val (completeOuters, completeInners, partialOuters, partialInners, completeUnknowns, partialUnknowns) = coords.foldLeft((List.empty[Line], List.empty[Line], List.empty[Line], List.empty[Line], List.empty[Line], List.empty[Line])) {
          case ((co, ci, po, pi, cu, pu), (role, geom: Line)) =>
            Option(geom) match {
              case Some(line) =>
                role match {
                  case "outer" if line.isClosed => (co :+ line, ci, po, pi, cu, pu)
                  case "outer" => (co, ci, po :+ line, pi, cu, pu)
                  case "inner" if line.isClosed => (co, ci :+ line, po, pi, cu, pu)
                  case "inner" => (co, ci, po, pi :+ line, cu, pu)
                  case "" if line.isClosed => (co, ci, po, pi, cu :+ line, pu)
                  case "" => (co, ci, po, pi, cu, pu :+ line)
                  case _ => (co, ci, po, pi, cu, pu)
                }
              case None => (co, ci, po, pi, cu, pu)
            }
        }

        val unknowns: List[Line] = completeUnknowns ++ connectSegments(partialUnknowns.sortWith(_.length > _.length))

        val (outers, inners) = unknowns.foldLeft((completeOuters ++ connectSegments(partialOuters.sortWith(_.length > _.length)), completeInners ++ connectSegments(partialInners.sortWith(_.length > _.length)))) {
          case ((o: List[Line], i: List[Line]), u) =>
            if (o.exists(Polygon(_).contains(u))) {
              (o, i :+ u)
            } else {
              (o :+ u, i)
            }
        }

        // reclassify rings according to their topology (ignoring roles)
        val (classifiedOuters, classifiedInners) = (outers ++ inners).map(Polygon(_)).sortWith(_.area > _.area) match {
          case h :: t => t.foldLeft((List(h), List.empty[Polygon])) {
            case ((os, is), ring) =>
              ring match {
                // there's an outer ring that contains this one; this is an inner ring
                case _ if os.exists(or => or.contains(ring)) => (os, is :+ ring)
                case _ => (os :+ ring, is)
              }
          }
          case Nil => (List.empty[Polygon], List.empty[Polygon])
        }

        val (dissolvedOuters, addlInners) = dissolveRings(classifiedOuters)
        val (dissolvedInners, addlOuters) = dissolveRings(classifiedInners.map(_.exterior) ++ addlInners)

        val (polygons, _) = (dissolvedOuters ++ addlOuters)
          // sort by size (descending) to use rings as part of the largest available polygon
          .sortWith(Polygon(_).area > Polygon(_).area)
          // only use inners once if they're contained by multiple outer rings
          .foldLeft((List.empty[Polygon], dissolvedInners)) {
          case ((ps, is), (outer)) =>
            (ps :+ Polygon(outer, is.filter(inner => Polygon(outer).contains(inner))), is.filterNot(inner => Polygon(outer).contains(inner)))
        }

        polygons match {
          case p :: Nil => p.toWKB(4326)
          case ps => MultiPolygon(ps).toWKB(4326)
        }
      }
    } catch {
      case e: Throwable =>
        logger.warn(s"Could not reconstruct relation $id @ $version ($timestamp): $e")
        null
    }
  })

  def reconstructRelationGeometries(relations: DataFrame, geoms: DataFrame): DataFrame = {
    import relations.sparkSession.implicits._

    // TODO use max(relations("timestamp"), wayGeoms("timestamp")) as the assigned timestamp
    val members = relations
      .where(isMultiPolygon('tags))
      .select('changeset, 'id, 'version, 'timestamp, posexplode_outer('members).as(Seq("idx", "member")))
      .select(
        'changeset,
        'id,
        'version,
        'timestamp,
        'idx,
        'member.getField("type").as("type"),
        'member.getField("ref").as("ref"),
        'member.getField("role").as("role")
      )
      .join(geoms.select('type, 'id.as("ref"), 'updated, 'validUntil, 'geom), Seq("type", "ref"), "outer")
      .where(
        'geom.isNull or // allow null geoms through so we can check data validity later
          (geoms("updated") <= relations("timestamp") and relations("timestamp") < coalesce(geoms("validUntil"), current_timestamp)))

    members
      .groupBy('changeset, 'id, 'version, 'timestamp)
      .agg(collect_list(struct('idx, 'type, 'ref, 'role, 'geom)).as('parts))
      .select('id, 'version, 'timestamp, 'changeset, buildMultiPolygon('parts, 'id, 'version, 'timestamp).as("geom"))
  }

  def geometriesByRegion(nodeGeoms: Dataset[Row], wayGeoms: Dataset[Row]): DataFrame = {
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
      .agg(collect_set('country).as('countries))

  }

}
