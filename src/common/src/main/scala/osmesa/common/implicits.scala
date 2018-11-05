package osmesa.common

import com.vividsolutions.jts.{geom => jts}
import geotrellis.vector.Extent
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.locationtech.geomesa.spark.jts._
import osmesa.common.encoders._
import osmesa.common.functions.osm.isArea
import osmesa.common.impl.SnapshotWithTimestamp
import osmesa.common.traits._

import scala.reflect.runtime.universe.TypeTag

object implicits {
  // because import ds.sparkSession.implicits._ in traits fails to compile:
  private val ss: SparkSession = SparkSession.builder.getOrCreate()
  import ss.implicits._
  ss.withJTS

  // extensions that convert between types

  trait AsPointsExtension[I <: Coordinates with Identity with Tags with VersionControl,
                          O <: OSMFeature[jts.Point],
                          H >: History] {
    val ds: Dataset[I] with H

    /** Convert coordinates to geometries.
      *
      * @return Points with matching metadata for nodes with tags.
      */
    def withGeometry[R >: O: TypeTag]: Dataset[R] with H = asPoints

    /** Convert tagged nodes to Points.
      *
      * @return Points with matching metadata.
      */
    def asPoints[R >: O: TypeTag]: Dataset[R] with H = {
      implicit val encoder: Encoder[R] = buildEncoder[R]

      ds.where(size('tags) > 0)
        .asPointsInternal
        .as[R]
        .asInstanceOf[Dataset[R] with H]
    }
  }

  trait NodesExtension[I <: PackedType, O <: Node, H >: History] {
    val ds: Dataset[I] with H
    def nodes[R >: O: TypeTag]: Dataset[R] with H = {
      implicit val encoder: Encoder[R] = buildEncoder[R]

      ds.where('type === NodeType)
        .drop('type)
        .drop('nds)
        .drop('members)
        .as[R]
        .asInstanceOf[Dataset[R] with H]
    }
  }

  trait RelationsExtension[I <: PackedType, O <: Relation, H >: History] {
    val ds: Dataset[I] with H
    def relations[R >: O: TypeTag]: Dataset[R] with H = {
      implicit val encoder: Encoder[R] = buildEncoder[R]

      ds.where('type === RelationType)
        .drop('type)
        .drop('lat)
        .drop('lon)
        .drop('nds)
        .as[R]
        .asInstanceOf[Dataset[R] with H]
    }
  }

  trait WaysExtension[I <: PackedType, O <: Way, H >: History] {
    val ds: Dataset[I] with H
    def ways[R >: O: TypeTag]: Dataset[R] with H = {
      implicit val encoder: Encoder[R] = buildEncoder[R]

      ds.where('type === WayType)
        .drop('type)
        .drop('lat)
        .drop('lon)
        .drop('members)
        .as[R]
        .asInstanceOf[Dataset[R] with H]
    }
  }

  trait WithGeometryChangedExtension[I <: Coordinates with Identity, O, H >: History] {
    val ds: Dataset[I] with H

    /** Add a flag indicating whether the geometry changed between node versions.
      *
      * @return Elements with a `geometryChanged` column.
      */
    def withGeometryChanged[R >: O with GeometryChanged: TypeTag]: Dataset[R] with H = {
      implicit val encoder: Encoder[R] = buildEncoder[R]

      ds.withGeometryChangedInternal
        .as[R]
        .asInstanceOf[Dataset[R] with H]
    }
  }

  trait WithGeometryExtension[I <: Way with Timestamp] {
    val ds: Dataset[I] with History

    /** Reconstruct geometries.
      *
      * @return Ways with matching metadata.
      */
    def withGeometry[
        R >: OSMFeature[jts.Geometry] with GeometryChanged with MinorVersion with Tags with Validity: TypeTag](
        implicit nodes: Dataset[Node with Timestamp] with History): Dataset[R] with History = {
      implicit val encoder: Encoder[R] = buildEncoder[R]
      reconstruction
        .reconstructWayGeometries(ds.withValidity.withArea, nodes.withValidity.withGeometryChanged)
        .as[R]
        .asInstanceOf[Dataset[R] with History]
//      reconstruction.reconstructWays(
//        ds.withValidity.withArea,
//        nodes.withGeometryChanged
//          .where('geometryChanged)
//          .drop('geometryChanged)
//          .asInstanceOf[Dataset[Node with Timestamp] with History]
//          .withValidity
//      )
    }
  }

  trait WithValidityExtension[I <: Identity with Tags with Timestamp, O, H >: History] {
    val ds: Dataset[I] with H

    /** Add validity windows to elements.
      *
      * `timestamp` is changed to `updated` (to prepare for timestamps inherited from contributing elements),
      * `validUntil` is introduced based on the creation `timestamp` of the next version. Elements with a `null`
      * `validUntil` are currently valid.
      *
      *
      * Tags are also copied from elements immediately prior to being deleted (!'visible), as they're cleared out
      * otherwise.
      *
      * @return Elements with `timestamp` and `validUntil` columns.
      */
    def withValidity[R >: O with Validity: TypeTag]: Dataset[R] with H = {
      implicit val encoder: Encoder[R] = buildEncoder[R]

      ds.withValidityInternal
        .as[R]
        .asInstanceOf[Dataset[R] with H]
    }
  }

  // extensions that (optionally) append types

  implicit class CoordinatesWithGeometryChangedExtension[T <: Coordinates with Identity,
  H >: History](ds: Dataset[T] with H) {

    private[osmesa] def withGeometryChangedInternal: DataFrame = {
      @transient val idByVersion: WindowSpec = Window.partitionBy('id).orderBy('version)

      ds.withColumn(
        "geometryChanged",
        !((lag('lat, 1) over idByVersion) <=> 'lat and (lag('lon, 1) over idByVersion) <=> 'lon)
      )
//        (lag('lat, 1) over idByVersion) =!= 'lat or (lag('lon, 1) over idByVersion) =!= 'lon)
    }
  }

  implicit class FilterExtension[T, H >: History](ds: Dataset[T] with H)(
      implicit evidence: T <:< Coordinates) {
    def filter(extent: Extent): Dataset[T] with H = {
      ds.where('lon >= extent.xmin and 'lon <= extent.xmax)
        .where('lat >= extent.ymin and 'lat <= extent.ymax)
        .asInstanceOf[Dataset[T] with H]
    }
  }

  implicit class NdsWithGeometryChangedExtension[T <: Nds, H >: History](ds: Dataset[T] with H) {

    /** Add a flag indicating whether the geometry changed between way versions.
      *
      * This only checks whether the list of nodes was modified, so it's inconclusive (nodes may have moved between way
      * versions, in which case they don't contribute directly).
      *
      * @return Elements with a `geometryChanged` column.
      */
    def withGeometryChanged[R >: T with GeometryChanged: TypeTag]: Dataset[R] with H = ???
  }

  implicit class MembersWithGeometryChangedExtension[T <: Members, H >: History](
      ds: Dataset[T] with H) {

    /** Add a flag indicating whether the geometry changed between relation versions.
      *
      * This only checks whether the list of members was modified, so it's inconclusive (members may have been modified
      * between relation versions, in which case they don't contribute directly).
      *
      * @return Elements with a `geometryChanged` column.
      */
    def withGeometryChanged[R >: T with GeometryChanged: TypeTag]: Dataset[R] with H = ???
  }

  implicit class SnapshotExtension(history: Dataset[OSM] with History) {
    def snapshot(timestamp: java.sql.Timestamp): Snapshot[Dataset[OSM]] with Timestamp = {
      implicit val encoder: Encoder[OSM] = buildEncoder[OSM]

      val valid = history.withValidityInternal
        .where(
          'updated <= coalesce(lit(timestamp), current_timestamp)
            and coalesce(lit(timestamp), current_timestamp) < coalesce('validUntil,
                                                                       lit(IndefiniteFuture)))
        .withColumnRenamed("updated", "timestamp")
        .drop('validUntil)
        .as[OSM]

      SnapshotWithTimestamp(valid, timestamp)
    }
  }

  implicit class WithAreaExtension[T <: Tags, H >: History](ds: Dataset[T] with H) {
    def withArea[R >: T with Area: TypeTag]: Dataset[R] with H = {
      implicit val encoder: Encoder[R] = buildEncoder[R]

      ds.withColumn("isArea", isArea('tags))
        .as[R]
        .asInstanceOf[Dataset[R] with H]
    }
  }

  // concrete type extensions

  implicit class ChangesetExtension[T <: Changeset: TypeTag](changesets: Dataset[T]) {
    import changesets.sparkSession.implicits._

    def extractFor[U <: VersionControl](ds: Dataset[U]): Dataset[T] = {
      implicit val encoder: Encoder[T] = buildEncoder[T]

      ds.select('changeset)
        .distinct
        .withColumnRenamed("changeset", "id")
        .join(changesets, Seq("id"))
        .as[T]
    }
  }

  implicit class IdentityWithTagsWithTimestampExtension[T <: Identity with Tags with Timestamp,
  H >: History](val ds: Dataset[T] with H) {
    private[osmesa] def withValidityInternal: DataFrame = {
      @transient val idByVersion: WindowSpec = Window.partitionBy('id).orderBy('version)

      ds.withColumn("tags",
                    when(!'visible and (lag('tags, 1) over idByVersion).isNotNull,
                         lag('tags, 1) over idByVersion).otherwise('tags))
        .withColumnRenamed("timestamp", "updated")
        .withColumn("validUntil", lead('updated, 1) over idByVersion)
    }
  }

  implicit class IdentityWithVersionControlExtension[T <: Identity with VersionControl](
      ds: Dataset[T]) {

    implicit val encoder: Encoder[Row] = RowEncoder(ds.schema)

    // TODO create a version of this that groups by timestamp (configurable granularity, e.g. 1 day) when
    // VersionControl is absent (or unreliable, in the case of GDPR-restricted GeoFabrik extracts where changeset is
    // always 0)
    private[osmesa] def asPointsInternal: DataFrame = {
      // this needs to convert to DataFrames first in order to avoid narrowing the schema produced

      ds.toDF()
        .groupByKey(n => (n.getAs[Long]("changeset"), n.getAs[Long]("id")))
        .reduceGroups((a, b) => {
          if (a.getAs[Int]("version") > b.getAs[Int]("version")) {
            a
          } else {
            b
          }
        })
        .map(_._2)
        .withColumn("type", lit(NodeType))
        .withColumn("geom", st_makePoint('lon, 'lat))
    }
  }

  // extensions that convert between types

  implicit class OSMExtension[T <: OSM, H >: History](val ds: Dataset[T] with H)
      extends NodesExtension[T, Node with Timestamp, H]
      with RelationsExtension[T, Relation with Timestamp, H]
      with WaysExtension[T, Way with Timestamp, H]
      with WithGeometryChangedExtension[T, UniversalElement, H]
      with WithValidityExtension[T, UniversalElement, H]

  implicit class NodeExtension[T <: Node, H >: History](val ds: Dataset[T] with H)
      extends AsPointsExtension[T, OSMFeature[jts.Point] with Metadata with Visibility, H]

  implicit class NodeWithTimestampExtension[T <: Node with Timestamp, H >: History](
      val ds: Dataset[T] with H)
      extends AsPointsExtension[T,
                                OSMFeature[jts.Point] with Metadata with Timestamp with Visibility,
                                H]
      with WithGeometryChangedExtension[T, T, H]
      with WithValidityExtension[T, Node, H]

  implicit class NodeWithValidityExtension[T <: Node with Validity, H >: History](
      val ds: Dataset[T] with H)
      extends AsPointsExtension[T,
                                OSMFeature[jts.Point] with Metadata with Validity with Visibility,
                                H]
      with WithGeometryChangedExtension[T, T, H]

  implicit class RelationWithTimestampExtension[T <: Relation with Timestamp, H >: History](
      val ds: Dataset[T] with H)
      extends WithValidityExtension[T, Relation, H]

  implicit class HistoricalWayWithTimestampExtension[T <: Way with Timestamp](
      val ds: Dataset[T] with History)
      extends WithGeometryExtension[T]
      with WithValidityExtension[T, Way, History]
}
