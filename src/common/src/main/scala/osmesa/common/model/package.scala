package osmesa.common

import geotrellis.raster.{Tile, Raster => GTRaster}
import geotrellis.vector.io._
import geotrellis.vector.{Feature, Point, Geometry => GTGeometry}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, Encoder => SparkEncoder}
import org.apache.spark.sql.jts._

package object model {
  type AugmentedDiffType = (Option[AugmentedDiffFeature], AugmentedDiffFeature)
  type AugmentedDiffFeature = Feature[GTGeometry, ElementWithSequence]

  def AugmentedDiffSchema = StructType(
    StructField("sequence", LongType) ::
      StructField("_type", ByteType, nullable = false) ::
      StructField("id", LongType, nullable = false) ::
      StructField("prevGeom", GeometryUDT, nullable = true) ::
      StructField("geom", GeometryUDT, nullable = true) ::
      StructField(
      "prevTags",
      MapType(StringType, StringType, valueContainsNull = false),
      nullable = true
    ) ::
      StructField(
      "tags",
      MapType(StringType, StringType, valueContainsNull = false),
      nullable = false
    ) ::
      StructField("prevChangeset", LongType, nullable = true) ::
      StructField("changeset", LongType, nullable = false) ::
      StructField("prevUid", LongType, nullable = true) ::
      StructField("uid", LongType, nullable = false) ::
      StructField("prevUser", StringType, nullable = true) ::
      StructField("user", StringType, nullable = false) ::
      StructField("prevUpdated", TimestampType, nullable = true) ::
      StructField("updated", TimestampType, nullable = false) ::
      StructField("prevVisible", BooleanType, nullable = true) ::
      StructField("visible", BooleanType, nullable = false) ::
      StructField("prevVersion", IntegerType, nullable = true) ::
      StructField("version", IntegerType, nullable = false) ::
      StructField("prevMinorVersion", IntegerType, nullable = true) ::
      StructField("minorVersion", IntegerType, nullable = false) ::
      Nil
  )

  def ChangesetSchema = StructType(
    StructField("sequence", IntegerType) ::
      StructField("id", LongType) ::
      StructField("created_at", TimestampType, nullable = false) ::
      StructField("closed_at", TimestampType, nullable = true) ::
      StructField("open", BooleanType, nullable = false) ::
      StructField("num_changes", IntegerType, nullable = false) ::
      StructField("user", StringType, nullable = false) ::
      StructField("uid", LongType, nullable = false) ::
      StructField("min_lat", FloatType, nullable = true) ::
      StructField("max_lat", FloatType, nullable = true) ::
      StructField("min_lon", FloatType, nullable = true) ::
      StructField("max_lon", FloatType, nullable = true) ::
      StructField("comments_count", IntegerType, nullable = false) ::
      StructField(
      "tags",
      MapType(StringType, StringType, valueContainsNull = false),
      nullable = false
    ) ::
      Nil
  )

  def ChangeSchema = StructType(
    StructField("sequence", IntegerType) ::
      StructField("_type", ByteType, nullable = false) ::
      StructField("id", LongType, nullable = false) ::
      StructField(
      "tags",
      MapType(StringType, StringType, valueContainsNull = false),
      nullable = false
    ) ::
      StructField("lat", DataTypes.createDecimalType(9, 7), nullable = true) ::
      StructField("lon", DataTypes.createDecimalType(10, 7), nullable = true) ::
      StructField("nds", DataTypes.createArrayType(LongType), nullable = true) ::
      StructField(
      "members",
      DataTypes.createArrayType(
        StructType(
          StructField("_type", ByteType, nullable = false) ::
            StructField("ref", LongType, nullable = false) ::
            StructField("role", StringType, nullable = false) ::
            Nil
        )
      ),
      nullable = true
    ) ::
      StructField("changeset", LongType, nullable = false) ::
      StructField("timestamp", TimestampType, nullable = false) ::
      StructField("uid", LongType, nullable = false) ::
      StructField("user", StringType, nullable = false) ::
      StructField("version", IntegerType, nullable = false) ::
      StructField("visible", BooleanType, nullable = false) ::
      Nil
  )

  def AugmentedDiffEncoder: SparkEncoder[Row] = RowEncoder(AugmentedDiffSchema)

  trait Geometry {
    def geom: GTGeometry
  }

  trait SerializedGeometry extends Geometry {
    lazy val geom: GTGeometry = wkb.readWKB

    def wkb: Array[Byte]
  }

  trait TileCoordinates {
    def zoom: Int
    def x: Int
    def y: Int
  }

  trait GeometryTile extends SerializedGeometry with TileCoordinates

  trait Raster {
    def raster: GTRaster[Tile]
  }

  trait RasterTile extends Raster with TileCoordinates

  trait Coordinates extends Geometry {
    def lat: Option[Double]
    def lon: Option[Double]

    def geom: Point = Point(x, y)

    def x: Float = lon.map(_.floatValue).getOrElse(Float.NaN)
    def y: Float = lat.map(_.floatValue).getOrElse(Float.NaN)
  }

  trait Key {
    def key: String
  }

  trait Sequence {
    def sequence: Int
  }

  // NOTE this doesn't extend TileSeq[T] to avoid using type parameters
  trait RasterWithSequenceTileSeq {
    def tiles: Seq[Raster with Sequence]
  }

  trait Count {
    def count: Long
  }
}
