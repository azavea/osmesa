package osmesa.common

import com.vividsolutions.jts.{geom => jts}
import org.apache.spark.sql.Dataset
import osmesa.common.model.ChangesetComment

package object traits {
  // TODO replication sequence (see osmesa.common.model)

  /** Does this element represent an area? */
  trait Area {
    def isArea: Boolean
  }

  /** Contains information about the user responsible for this element.
    *
    * Note: `user` is not a stable identifier; it may be changed at-will.
    *
    * Availability may be subject to GDPR guidelines.
    */
  trait Authorship {
    def uid: Long
    def user: String
  }

  /** Description associated with a group of element versions. */
  trait Changeset extends Authorship {
    def id: Long
    def createdAt: java.sql.Timestamp
    def closedAt: Option[java.sql.Timestamp]
    def open: Boolean
    def numChanges: Int
    def minLat: Option[Double]
    def maxLat: Option[Double]
    def minLon: Option[Double]
    def maxLon: Option[Double]
    def commentsCount: Int
  }

  /** Latitude / longitude.
    *
    * Typically part of a node.
    */
  trait Coordinates {
    def lat: Option[Double]
    def lon: Option[Double]
  }

  /** Comments associated with a Changeset. */
  trait Comments {
    def comments: Seq[ChangesetComment]
  }

  /** A uniquely identified set of tags describing an element. */
  trait Element extends Identity with Tags

  /** Geometric representation of an element. */
  trait Geometry {
    def geom: jts.Geometry
  }

  /** Has this element's geometry changed relative to its previous version? */
  trait GeometryChanged {
    def geometryChanged: Boolean
  }

  /** Indicates that multiple versions of individual elements may be present.
    *
    * Typically applied to Dataset[T].
    */
  trait History

  /** Uniquely describes a concrete element. */
  trait Identity {
    def id: Long
    def version: Int
  }

  /** Contains a list of `members` (references to elements).
    *
    * Typically part of a relation.
    */
  trait Members {
    def members: Seq[Member]
  }

  /** Contains metadata. */
  trait Metadata extends VersionControl with Authorship

  /** Contains `minorVersion`. */
  trait MinorVersion {
    def minorVersion: Int
  }

  /** Contains a list of `nds` (references to Nodes).
    *
    * Typically part of a way.
    */
  trait Nds {
    def nds: Seq[Long]
  }

  /** An OSM node. */
  trait Node extends Element with Coordinates with Metadata with Visibility

  /** OSM data as represented by a planet dump. */
  trait OSM
      extends Element
      with PackedType
      with Coordinates
      with Nds
      with Members
      with Metadata
      with Timestamp
      with Visibility

  /** An OSM element with its geometric representation. */
  trait OSMFeature extends Identity with PackedType with Geometry with VersionControl

  /** Contains `type` as a Byte to reduce storage requirements. */
  trait PackedType {
    def `type`: Byte
  }

  /** An OSM relation. */
  trait Relation extends Element with Members with Metadata with Visibility

  /** A snapshot of OSM data containing individual versions of elements present. */
  trait Snapshot[T <: Dataset[_]] {
    def dataset: T
  }

  /** Key/value pairs describing this element. */
  trait Tags {
    def tags: scala.collection.Map[String, String]
  }

  /** Contains a timestamp corresponding to when an element was edited or snapshot taken. */
  trait Timestamp {
    def timestamp: java.sql.Timestamp
  }

  /** An instance of any OSM element. */
  trait UniversalElement extends Node with Way with Relation

  /** Contains a range for which the element was considered valid. */
  trait Validity {
    def updated: java.sql.Timestamp
    def validUntil: Option[java.sql.Timestamp]
  }

  /** Contains a `changeset` property used to logically group edits together.
    *
    * Availability may be subject to GDPR guidelines.
    */
  trait VersionControl {
    def changeset: Long
  }

  /** Is this element visible? */
  trait Visibility {
    def visible: Boolean
  }

  /** An OSM way. */
  trait Way extends Element with Nds with Metadata with Visibility

  /** A reference to an element.
    *
    * Typically used as entries in a list of relation members.
    */
  final case class Member(`type`: Byte, ref: Long, role: String)
}
