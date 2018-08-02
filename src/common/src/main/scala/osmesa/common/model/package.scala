package osmesa.common

import java.net.URI
import java.time.Instant

import cats.syntax.either._
import geotrellis.vector.{Feature, Geometry}
import io.circe._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, Encoder => SparkEncoder}

import scala.util.Random

package object model {
  type AugmentedDiffFeature = Feature[Geometry, ElementWithSequence]

  val AugmentedDiffSchema = StructType(
    StructField("sequence", LongType) ::
      StructField("_type", ByteType, nullable = false) ::
      StructField("id", LongType, nullable = false) ::
      StructField("prevGeom", BinaryType, nullable = true) ::
      StructField("geom", BinaryType, nullable = true) ::
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

  val ChangesetSchema = StructType(
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

  val ChangeSchema = StructType(
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

  val AugmentedDiffEncoder: SparkEncoder[Row] = RowEncoder(AugmentedDiffSchema)

  implicit class RandomGetSeq[A](lst: Seq[A]) {
    def takeRandom: Option[A] = lst.lift(Random.nextInt(lst.size))
  }

  implicit class RandomGetArray[A](lst: Array[A]) {
    def takeRandom: Option[A] = lst.lift(Random.nextInt(lst.size))
  }

  implicit val encodeInstant: Encoder[Instant] =
    Encoder.encodeString.contramap[Instant](_.toString)

  implicit val decodeInstant: Decoder[Instant] = Decoder.decodeString.emap {
    str => Either.catchNonFatal(Instant.parse(str)).leftMap(t => "Instant")
  }

  implicit val encodeURI: Encoder[URI] =
    Encoder.encodeString.contramap[URI](_.toString)

  implicit val decodeURI: Decoder[URI] = Decoder.decodeString.emap { str =>
    Either.catchNonFatal(new URI(str)).leftMap(t => "URI")
  }
}
