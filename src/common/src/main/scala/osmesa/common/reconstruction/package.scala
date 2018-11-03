package osmesa.common
import java.sql

import com.vividsolutions.jts.{geom => jts}
import geotrellis.vector._
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import osmesa.common.traits._

package object reconstruction extends Logging {

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
  def reconstructWayGeometries(ways: Dataset[Way with Validity with Area],
                               _nodes: Dataset[Node with Validity with GeometryChanged],
                               _nodesToWays: Option[DataFrame] = None): DataFrame = {
    import ways.sparkSession.implicits._

    @transient val idByVersion = Window.partitionBy('id).orderBy('version)

    val nodes = _nodes
      .where('geometryChanged)
      .drop('geometryChanged)
      // re-calculate validUntil windows
      .withColumn("validUntil", lead('updated, 1) over idByVersion)

    // Create (or re-use) a lookup table for node → ways
    val nodesToWays = _nodesToWays.getOrElse[DataFrame] {
      ways
        .select(explode('nds) as 'id, 'id as 'wayId, 'version, 'updated as 'timestamp, 'validUntil)
    }

    // Create a way entry for each changeset in which a node was modified, containing the timestamp of the node that
    // triggered the association. This will later be used to assemble ways at each of those points in time. If you need
    // authorship, join on changesets
    // TODO check on partitioning of nodes (assume that the thing requesting the join gets to keep its partitioning)
    val waysByChangeset = nodes
      .select('changeset, 'id, 'updated)
      .join(nodesToWays, Seq("id"))
      .where('timestamp <= 'updated and 'updated < coalesce('validUntil, current_timestamp))
      .select('changeset, 'wayId as 'id, 'version, 'updated)

    val allWayVersions = waysByChangeset
    // Union with raw ways to include those in the time line (if they weren't already triggered by node modifications
    // at the same time)
      .union(ways.select('changeset, 'id, 'version, 'updated))
      // If a node and a way were modified within the same changeset at different times, there will be multiple entries
      // per changeset (with different timestamps). There should only be one per changeset.
      .groupBy('changeset, 'id)
      .agg(max('version).cast(IntegerType) as 'version, max('updated) as 'updated)
      .join(ways.select('id, 'version, 'nds, 'isArea), Seq("id", "version"))

    val explodedWays = allWayVersions
      .select('changeset,
              'id,
              'version,
              'updated,
              'isArea,
              posexplode_outer('nds) as Seq("idx", "ref"))
      // repartition including updated timestamp to avoid skew (version is insufficient, as
      // multiple instances may exist with the same version)
      .repartition('id, 'updated)

    val waysAndNodes = explodedWays
      .join(nodes.select('id as 'ref, 'updated as 'timestamp, 'validUntil, 'lat, 'lon),
            Seq("ref"),
            "left_outer")
      .where('timestamp <= 'updated and 'updated < coalesce('validUntil, current_timestamp))

    implicit val encoder: Encoder[Row] = ProcessOSM.BareElementEncoder

    val wayGeoms = waysAndNodes
      .select('changeset, 'id, 'version, 'updated, 'isArea, 'idx, 'lat, 'lon)
      .groupByKey(
        row =>
          (row.getAs[Long]("changeset"),
           row.getAs[Long]("id"),
           row.getAs[Integer]("version"),
           row.getAs[java.sql.Timestamp]("updated")))
      .mapGroups {
        case ((changeset, id, version, updated), rows) =>
          val nds = rows.toVector
          val isArea = nds.head.getAs[Boolean]("isArea")
          val geom = nds
            .sortWith((a, b) => a.getAs[Int]("idx") < b.getAs[Int]("idx"))
            .map { row =>
              Seq(
                Option(row.get(row.fieldIndex("lon")))
                  .map(_.asInstanceOf[Double])
                  .getOrElse(Double.NaN),
                Option(row.get(row.fieldIndex("lat")))
                  .map(_.asInstanceOf[Double])
                  .getOrElse(Double.NaN)
              )
            } match {
            // no coordinates provided
            case coords if coords.isEmpty =>
              Some(GeomFactory.factory.createLineString(Array.empty[jts.Coordinate]))
            // some of the coordinates are empty; this is invalid
            case coords if coords.exists(Option(_).isEmpty) => None
            // some of the coordinates are invalid
            case coords if coords.exists(_.exists(_.isNaN)) => None
            // 1 pair of coordinates provided
            case coords if coords.length == 1 =>
              Some(
                GeomFactory.factory.createPoint(
                  new jts.Coordinate(coords.head.head, coords.head.last)))
            case coords => {
              val coordinates = coords.map(xy => new jts.Coordinate(xy.head, xy.last)).toArray
              val line = GeomFactory.factory.createLineString(coordinates)

              if (isArea && line.getNumPoints >= 4 && line.isClosed)
                Some(GeomFactory.factory.createPolygon(line.getCoordinateSequence))
              else
                Some(line)
            }
          }
          val geometry = geom match {
            case Some(g) if g.isValid => g
            case _                    => null
          }
          new GenericRowWithSchema(Array(changeset, id, version, updated, geometry),
                                   ProcessOSM.BareElementSchema): Row
      }

    @transient val idAndVersionByUpdated = Window.partitionBy('id, 'version).orderBy('updated)
    @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

    wayGeoms
    // Assign `minorVersion` and rewrite `validUntil` to match
      .withColumn("validUntil", lead('updated, 1) over idByUpdated)
      .withColumn("minorVersion", (row_number over idAndVersionByUpdated) - 1)
      .withColumn("geometryChanged", !((lag('geom, 1) over idByUpdated) <=> 'geom))
      .join(ways.select('id, 'version, 'tags, 'visible), Seq("id", "version"))
      .select(lit(WayType) as 'type,
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
}
