package osmesa

import java.math.BigDecimal
import java.time.Instant

import com.vividsolutions.jts.{geom => jts}
import geotrellis.vector.Extent
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types._
import org.locationtech.geomesa.spark.jts._
import osmesa.common.implementations._
import osmesa.common.model.ChangesetComment
import osmesa.common.traits._

package object common {

  lazy val compressMemberTypes: UserDefinedFunction = udf(_compressMemberTypes, MemberSchema)
  val NodeType: Byte = 1
  val WayType: Byte = 2
  val RelationType: Byte = 3
  val IndefiniteFuture: java.sql.Timestamp = java.sql.Timestamp.from(Instant.MAX)

  implicit def stringTypeToByte(`type`: String): Byte = `type` match {
    case "node"     => NodeType
    case "way"      => WayType
    case "relation" => RelationType
  }

  implicit def packedTypeToString(`type`: Byte): String = `type` match {
    case NodeType     => "node"
    case WayType      => "way"
    case RelationType => "relation"
  }
  val asOptionalFloat: UserDefinedFunction = udf {
    Option(_: BigDecimal).map(_.floatValue)
  }
  val typeAsByte: UserDefinedFunction = udf { stringType: String => stringTypeToByte(stringType)
  }
  private val MemberSchema = ArrayType(
    StructType(
      StructField("type", ByteType, nullable = false) ::
        StructField("ref", LongType, nullable = false) ::
        StructField("role", StringType, nullable = false) ::
        Nil),
    containsNull = false
  )
  private val _compressMemberTypes = (members: Seq[Row]) =>
    members.map { row =>
      val `type` = row.getAs[String]("type")
      val ref = row.getAs[Long]("ref")
      val role = row.getAs[String]("role")

      Member(`type`, ref, role)
  }

  def asChangesets(df: DataFrame): Dataset[Changeset] = {
    import df.sparkSession.implicits._

    df.withColumnRenamed("created_at", "createdAt")
      .withColumnRenamed("closed_at", "closedAt")
      .withColumn("numChanges", 'num_changes.cast(IntegerType))
      .withColumn("minLat", asOptionalFloat('min_lat))
      .withColumn("maxLat", asOptionalFloat('max_lat))
      .withColumn("minLon", asOptionalFloat('min_lon))
      .withColumn("maxLon", asOptionalFloat('max_lon))
      .withColumn("commentsCount", 'comments_count.cast(IntegerType))
      .drop('num_changes)
      .drop('min_lat)
      .drop('max_lat)
      .drop('min_lon)
      .drop('max_lon)
      .drop('comments_count)
      .as[ChangesetImpl]
      .asInstanceOf[Dataset[Changeset]]
  }

  /** Adds type information to a DataFrame containing a historical OSM planet.
    *
    * Additional columns will be retained but will not be accessible through the [[traits.OSM]] interface.
    *
    * @param history Historical OSM planet.
    * @return Typed historical OSM planet.
    */
  def asHistory(history: DataFrame): Dataset[OSM] with History =
    asOSM(history).asInstanceOf[Dataset[OSM] with History]

  /** Adds type information to a DataFrame containing an OSM planet.
    *
    * Additional columns will be retained but will not be accessible through the [[traits.OSM]] interface.
    *
    * @param df OSM planet.
    * @return Typed OSM planet.
    */
  def asOSM(df: DataFrame): Dataset[OSM] = {
    import df.sparkSession.implicits._

    df
    // downcast to reduce storage requirements
      .withColumn("type", typeAsByte('type))
      .withColumn("lat", asOptionalFloat('lat))
      .withColumn("lon", asOptionalFloat('lon))
      .withColumn("members", compressMemberTypes('members))
      .withColumn("version", 'version.cast(IntegerType))
      // de-reference
      .withColumn("nds", $"nds.ref")
      .as[Planet]
      .asInstanceOf[Dataset[OSM]]
  }

  /** Adds type information to a DataFrame containing an OSM planet snapshot.
    *
    * It is up to the caller to ensure that this only contains individual element versions. Additional columns will be
    * retained but will not be accessible through the [[traits.OSM]] interface.
    *
    * @param snapshot OSM planet snapshot.
    * @return Typed OSM planet snapshot.
    */
  def asSnapshot(snapshot: DataFrame): Snapshot[Dataset[OSM]] =
    SnapshotImpl(asOSM(snapshot))

  object traits {
    // for internal representations that use bytes to represent types
    trait PackedType {
      def `type`: Byte
    }

    trait Nds {
      def nds: Seq[Long]
    }

    trait Members {
      def members: Seq[Member]
    }

    trait VersionControl {
      def changeset: Long // GDPR-restricted
    }

    trait Authorship {
      def uid: Long // GDPR-restricted
      def user: String // GDPR-restricted
    }

    trait Metadata extends VersionControl with Authorship

    // applied to Datasets containing individual element versions; this describes the timestamp for which they're va
    // can also be applied to individual types
    trait Timestamp {
      def timestamp: java.sql.Timestamp
    }

    trait Visibility {
      def visible: Boolean
    }

    trait Tags {
      def tags: scala.collection.Map[String, String]
    }

    trait Coordinates {
      def lat: Option[Float]
      def lon: Option[Float]
    }

    trait Identity {
      def id: Long
      def version: Int
    }

    trait Geometry {
      def geom: jts.Geometry
    }

    trait Element extends Identity with Tags

    trait Node extends Element with Coordinates with Metadata with Visibility

    trait Way extends Element with Nds with Metadata with Visibility

    trait Relation extends Element with Members with Metadata with Visibility

    trait UniversalElement extends Node with Way with Relation

    trait OSM
        extends Element
        with PackedType
        with Coordinates
        with Nds
        with Members
        with Metadata
        with Timestamp
        with Visibility

    // applied to Datasets containing individual element versions
    trait Snapshot[T <: Dataset[_]] {
      def dataset: T
    }

    trait History

    trait Validity {
      def updated: java.sql.Timestamp
      def validUntil: Option[java.sql.Timestamp]
    }

    trait Changeset {
      def id: Long
      def createdAt: java.sql.Timestamp
      def closedAt: Option[java.sql.Timestamp]
      def open: Boolean
      def numChanges: Int
      def user: String
      def uid: Long
      def minLat: Option[Float]
      def maxLat: Option[Float]
      def minLon: Option[Float]
      def maxLon: Option[Float]
      def commentsCount: Int
      def tags: Map[String, String]
    }

    trait Comments {
      def comments: Seq[ChangesetComment]
    }

    final case class Member(`type`: Byte, ref: Long, role: String)
  }

  object implementations {
    final case class Planet(id: Long,
                            `type`: Byte,
                            tags: Map[String, String],
                            lat: Option[Float],
                            lon: Option[Float],
                            nds: Seq[Long],
                            members: Seq[Member],
                            changeset: Long,
                            timestamp: java.sql.Timestamp,
                            uid: Long,
                            user: String,
                            version: Int,
                            visible: Boolean)
        extends OSM

    final case class UniversalElementWithValidity(id: Long,
                                                  `type`: String,
                                                  tags: Map[String, String],
                                                  lat: Option[Float],
                                                  lon: Option[Float],
                                                  nds: Seq[Long],
                                                  members: Seq[Member],
                                                  changeset: Long,
                                                  updated: java.sql.Timestamp,
                                                  validUntil: Option[java.sql.Timestamp],
                                                  uid: Long,
                                                  user: String,
                                                  version: Int,
                                                  visible: Boolean)
        extends UniversalElement
        with Validity

    final case class CoordinatesWithIdentityWithVersionControl(lat: Option[Float],
                                                               lon: Option[Float],
                                                               id: Long,
                                                               version: Int,
                                                               changeset: Long)
        extends Coordinates
        with Identity
        with VersionControl

    final case class NodeWithTimestamp(id: Long,
                                       tags: Map[String, String],
                                       lat: Option[Float],
                                       lon: Option[Float],
                                       changeset: Long,
                                       timestamp: java.sql.Timestamp,
                                       uid: Long,
                                       user: String,
                                       version: Int,
                                       visible: Boolean)
        extends Node
        with Timestamp

    final case class NodeWithValidity(id: Long,
                                      tags: Map[String, String],
                                      lat: Option[Float],
                                      lon: Option[Float],
                                      changeset: Long,
                                      updated: java.sql.Timestamp,
                                      validUntil: Option[java.sql.Timestamp],
                                      uid: Long,
                                      user: String,
                                      version: Int,
                                      visible: Boolean)
        extends Node
        with Validity

    final case class WayWithValidity(id: Long,
                                     tags: Map[String, String],
                                     nds: Seq[Long],
                                     changeset: Long,
                                     updated: java.sql.Timestamp,
                                     validUntil: Option[java.sql.Timestamp],
                                     uid: Long,
                                     user: String,
                                     version: Int,
                                     visible: Boolean)
        extends Way
        with Validity

    final case class RelationWithValidity(id: Long,
                                          tags: Map[String, String],
                                          members: Seq[Member],
                                          changeset: Long,
                                          updated: java.sql.Timestamp,
                                          validUntil: Option[java.sql.Timestamp],
                                          uid: Long,
                                          user: String,
                                          version: Int,
                                          visible: Boolean)
        extends Relation
        with Validity

    final case class WayWithTimestamp(id: Long,
                                      tags: Map[String, String],
                                      nds: Seq[Long],
                                      changeset: Long,
                                      timestamp: java.sql.Timestamp,
                                      uid: Long,
                                      user: String,
                                      version: Int,
                                      visible: Boolean)
        extends Way
        with Timestamp

    final case class RelationWithTimestamp(id: Long,
                                           tags: Map[String, String],
                                           members: Seq[Member],
                                           changeset: Long,
                                           timestamp: java.sql.Timestamp,
                                           uid: Long,
                                           user: String,
                                           version: Int,
                                           visible: Boolean)
        extends Relation
        with Timestamp

    final case class SnapshotWithTimestamp[T <: Dataset[_]](override val dataset: T,
                                                            timestamp: java.sql.Timestamp)
        extends SnapshotImpl(dataset)
        with Timestamp

    class SnapshotImpl[T <: Dataset[_]](val dataset: T) extends Snapshot[T]

    final case class ChangesetImpl(id: Long,
                                   createdAt: java.sql.Timestamp,
                                   closedAt: Option[java.sql.Timestamp],
                                   open: Boolean,
                                   numChanges: Int,
                                   user: String,
                                   uid: Long,
                                   minLat: Option[Float],
                                   maxLat: Option[Float],
                                   minLon: Option[Float],
                                   maxLon: Option[Float],
                                   commentsCount: Int,
                                   tags: Map[String, String])
        extends Changeset

    final case class PointWithTimestamp(`type`: Byte,
                                        id: Long,
                                        geom: jts.Point,
                                        tags: Map[String, String],
                                        changeset: Long,
                                        timestamp: java.sql.Timestamp,
                                        uid: Long,
                                        user: String,
                                        version: Int,
                                        visible: Boolean)
        extends Element
        with PackedType
        with Geometry
        with traits.Metadata
        with Timestamp
        with Visibility

    final case class PointImpl(`type`: Byte,
                               id: Long,
                               geom: jts.Point,
                               tags: Map[String, String],
                               changeset: Long,
                               uid: Long,
                               user: String,
                               version: Int,
                               visible: Boolean)
        extends Element
        with PackedType
        with Geometry
        with traits.Metadata
        with Visibility

    object SnapshotImpl {
      def apply[T <: Dataset[_]](dataset: T) = new SnapshotImpl[T](dataset)
    }
  }

  object implicits {
    implicit class HistoricalNodeWithTimestampDatasetExtension(
        history: Dataset[Node with Timestamp] with History) {
      import history.sparkSession.implicits._

      /** Add validity windows to nodes.
        *
        * `timestamp` is changed to `updated` (to prepare for timestamps inherited from contributing elements),
        * `validUntil` is introduced based on the creation `timestamp` of the next version. Elements with a `null`
        * `validUntil` are currently valid.
        *
        *
        * Tags are also copied from elements immediately prior to being deleted (!'visible), as they're cleared out
        * otherwise.
        *
        * @return Nodes with `timestamp` and `validUntil` columns.
        */
      def withValidity: Dataset[Node with Validity] with History =
        history.withValidityInternal
          .as[NodeWithValidity]
          .asInstanceOf[Dataset[Node with Validity] with History]
    }

    implicit class HistoricalWayWithTimestampDatasetExtension(
        history: Dataset[Way with Timestamp] with History) {
      import history.sparkSession.implicits._

      /** Add validity windows to ways.
        *
        * `timestamp` is changed to `updated` (to prepare for timestamps inherited from contributing elements),
        * `validUntil` is introduced based on the creation `timestamp` of the next version. Elements with a `null`
        * `validUntil` are currently valid.
        *
        *
        * Tags are also copied from elements immediately prior to being deleted (!'visible), as they're cleared out
        * otherwise.
        *
        * @return Ways with `timestamp` and `validUntil` columns.
        */
      def withValidity: Dataset[Way with Validity] with History =
        history.withValidityInternal
          .as[WayWithValidity]
          .asInstanceOf[Dataset[Way with Validity] with History]
    }

    implicit class HistoricalRelationWithTimestampDatasetExtension(
        history: Dataset[Relation with Timestamp] with History) {
      import history.sparkSession.implicits._

      /** Add validity windows to relations.
        *
        * `timestamp` is changed to `updated` (to prepare for timestamps inherited from contributing elements),
        * `validUntil` is introduced based on the creation `timestamp` of the next version. Elements with a `null`
        * `validUntil` are currently valid.
        *
        *
        * Tags are also copied from elements immediately prior to being deleted (!'visible), as they're cleared out
        * otherwise.
        *
        * @return Relations with `timestamp` and `validUntil` columns.
        */
      def withValidity: Dataset[Relation with Validity] with History =
        history.withValidityInternal
          .as[RelationWithValidity]
          .asInstanceOf[Dataset[Relation with Validity] with History]
    }

    implicit class IdentityWithTimestampDatasetExtension[T <: Identity with Timestamp](
        ds: Dataset[T]) {
      import ds.sparkSession.implicits._

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
      private[osmesa] def withValidityInternal: DataFrame = {
        @transient val idByVersion = Window.partitionBy('id).orderBy('version)

        ds.withColumn("tags",
                      when(!'visible and (lag('tags, 1) over idByVersion).isNotNull,
                           lag('tags, 1) over idByVersion).otherwise('tags))
          .withColumnRenamed("timestamp", "updated")
          .withColumn("validUntil", lead('updated, 1) over idByVersion)
      }
    }

    implicit class OSMDatasetExtension(ds: Dataset[OSM]) {
      import ds.sparkSession.implicits._

      def ways: Dataset[Way with Timestamp] =
        ds.where('type === WayType)
          .drop('type)
          .drop('lat)
          .drop('lon)
          .drop('members)
          .as[WayWithTimestamp]
          .asInstanceOf[Dataset[Way with Timestamp]]

      def relations: Dataset[Relation with Timestamp] =
        ds.where('type === RelationType)
          .drop('type)
          .drop('lat)
          .drop('lon)
          .drop('nds)
          .as[RelationWithTimestamp]
          .asInstanceOf[Dataset[Relation with Timestamp]]

      def nodes: Dataset[Node with Timestamp] =
        ds.where('type === NodeType)
          .drop('type)
          .drop('nds)
          .drop('members)
          .as[NodeWithTimestamp]
          .asInstanceOf[Dataset[Node with Timestamp]]
    }

    implicit class HistoricalOSMDatasetExtension(history: Dataset[OSM] with History) {
      import history.sparkSession.implicits._

      def ways: Dataset[Way with Timestamp] with History =
        history
          .where('type === WayType)
          .drop('type)
          .drop('lat)
          .drop('lon)
          .drop('members)
          .as[WayWithTimestamp]
          .asInstanceOf[Dataset[Way with Timestamp] with History]

      def relations: Dataset[Relation with Timestamp] with History =
        history
          .where('type === RelationType)
          .drop('type)
          .drop('lat)
          .drop('lon)
          .drop('nds)
          .as[RelationWithTimestamp]
          .asInstanceOf[Dataset[Relation with Timestamp] with History]

      def snapshot(timestamp: java.sql.Timestamp): Snapshot[Dataset[OSM]] with Timestamp = {
        val valid = history.withValidityInternal
          .where(
            'updated <= coalesce(lit(timestamp), current_timestamp)
              and coalesce(lit(timestamp), current_timestamp) < coalesce('validUntil,
                                                                         lit(IndefiniteFuture)))
          .withColumnRenamed("updated", "timestamp")
          .drop('validUntil)
          .as[Planet]
          .asInstanceOf[Dataset[OSM]]

        SnapshotWithTimestamp(valid, timestamp)
      }

      def nodes: Dataset[Node with Timestamp] with History =
        history
          .where('type === NodeType)
          .drop('type)
          .drop('nds)
          .drop('members)
          .as[NodeWithTimestamp]
          .asInstanceOf[Dataset[Node with Timestamp] with History]

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
      def withValidity: Dataset[UniversalElement with Validity] with History =
        history.withValidityInternal
          .as[UniversalElementWithValidity]
          .asInstanceOf[Dataset[UniversalElement with Validity] with History]
    }

    implicit class CoordinatesDatasetExtension[T <: Coordinates](nodes: Dataset[T]) {
      import nodes.sparkSession.implicits._
      nodes.sparkSession.withJTS

      def filter(extent: Extent): Dataset[T] = {
        nodes
          .where('lon >= extent.xmin and 'lon <= extent.xmax)
          .where('lat >= extent.ymin and 'lat <= extent.ymax)
      }
    }

    implicit class CoordinatesWithIdentityWithVersionControlDatasetExtension[
        T <: Coordinates with Identity with VersionControl](nodes: Dataset[T]) {
      import nodes.sparkSession.implicits._
      nodes.sparkSession.withJTS

      implicit val encoder: Encoder[Row] = RowEncoder(nodes.schema)

      // TODO create a version of this that groups by timestamp (configurable granularity, e.g. 1 day) when
      // VersionControl is absent (or unreliable, in the case of GDPR-restricted GeoFabrik extracts where changeset is
      // always 0)
      private[osmesa] def asPointsInternal: DataFrame = {
        // this needs to convert to DataFrames first in order to avoid narrowing the schema produced

        nodes
          .toDF()
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

    implicit class NodeDatasetExtension[T <: Node](nodes: Dataset[T]) {
      import nodes.sparkSession.implicits._

      /** Convert tagged nodes to Points.
        *
        * @return Points with matching metadata.
        */
      def asPoints
        : Dataset[Element with PackedType with Geometry with traits.Metadata with Visibility] = {
        nodes
          .where(size('tags) > 0)
          .asPointsInternal
          .as[PointImpl]
          .asInstanceOf[Dataset[
            Element with PackedType with Geometry with traits.Metadata with Visibility]]
      }
    }

    implicit class NodeWithTimestampDatasetExtension[T <: Node with Timestamp](nodes: Dataset[T]) {
      import nodes.sparkSession.implicits._
      nodes.sparkSession.withJTS

      implicit val NodeEncoder: Encoder[Node with Timestamp] =
        Encoders.product[NodeWithTimestamp].asInstanceOf[Encoder[Node with Timestamp]]

      /** Convert tagged nodes to Points.
        *
        * @return Points with matching metadata.
        */
      def asPoints: Dataset[
        Element with PackedType with Geometry with traits.Metadata with Timestamp with Visibility] = {
        nodes
          .where(size('tags) > 0)
          .asPointsInternal
          .as[PointWithTimestamp]
          .asInstanceOf[Dataset[
            Element with PackedType with Geometry with traits.Metadata with Timestamp with Visibility]]
      }
    }

    implicit class ChangesetDatasetExtension(changesets: Dataset[Changeset]) {
      import changesets.sparkSession.implicits._

      def extractFor[T <: VersionControl](ds: Dataset[T]): Dataset[Changeset] =
        ds.select('changeset)
          .distinct
          .withColumnRenamed("changeset", "id")
          .join(changesets, Seq("id"))
          .as[ChangesetImpl]
          .asInstanceOf[Dataset[Changeset]]
    }

    implicit val OSMEncoder: Encoder[OSM] = Encoders.product[Planet].asInstanceOf[Encoder[OSM]]
  }
}
