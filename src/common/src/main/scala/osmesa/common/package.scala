package osmesa

import java.math.BigDecimal
import java.time.Instant

import com.vividsolutions.jts.{geom => jts}
import org.apache.spark.sql.{TypedColumn, _}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.locationtech.geomesa.spark.jts._
import org.locationtech.geomesa.spark.jts.util.SQLFunctionHelper.udfToColumn
import osmesa.common.encoders._
import osmesa.common.impl.Snapshot
import osmesa.common.traits._

import scala.reflect.runtime.universe.TypeTag

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
  val asOptionalDouble: UserDefinedFunction = udf {
    Option(_: BigDecimal).map(_.doubleValue)
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
  private val ss: SparkSession = SparkSession.builder.getOrCreate()
  ss.withJTS
  private val ST_CastToGeometry: jts.Geometry => jts.Geometry = g => g

  private val castingNames = Map(
    ST_CastToGeometry -> "st_castToGeometry"
  )
  def st_castToGeometry(geom: Column): TypedColumn[Any, jts.Geometry] =
    udfToColumn(ST_CastToGeometry, castingNames, geom)

  /** Adds type information to a DataFrame containing changesets.
    *
    * Additional columns will be retained but will not be accessible through the [[traits.Changeset]] interface.
    *
    * @param df Changesets.
    * @return Typed changesets.
    */
  def asChangesets[R >: Changeset with Tags: TypeTag](df: DataFrame): Dataset[R] = {
    import df.sparkSession.implicits._

    implicit val encoder: Encoder[R] = buildEncoder[R]

    df.withColumnRenamed("created_at", "createdAt")
      .withColumnRenamed("closed_at", "closedAt")
      .withColumn("numChanges", 'num_changes.cast(IntegerType))
      .withColumn("minLat", asOptionalDouble('min_lat))
      .withColumn("maxLat", asOptionalDouble('max_lat))
      .withColumn("minLon", asOptionalDouble('min_lon))
      .withColumn("maxLon", asOptionalDouble('max_lon))
      .withColumn("commentsCount", 'comments_count.cast(IntegerType))
      .drop('num_changes)
      .drop('min_lat)
      .drop('max_lat)
      .drop('min_lon)
      .drop('max_lon)
      .drop('comments_count)
      .repartition('id)
      .as[R]
  }

  /** Adds type information to a DataFrame containing a historical OSM planet.
    *
    * Node coordinates are represented as doubles to preserve precision (OSM supports accuracy to 7 decimal places;
    * floats drop to 5 at extreme longitudes). Additional columns will be retained but will not be accessible through
    * the [[traits.OSM]] interface.
    *
    * @param history Historical OSM planet.
    * @return Typed historical OSM planet.
    */
  def asHistory(history: DataFrame): Dataset[OSM] with History =
    asOSM(history).asInstanceOf[Dataset[OSM] with History]

  /** Adds type information to a DataFrame containing an OSM planet snapshot.
    *
    * It is up to the caller to ensure that this only contains individual element versions. Node coordinates are
    * represented as doubles to preserve precision (OSM supports accuracy to 7 decimal places; * floats drop to 5 at
    * extreme longitudes). Additional columns will be retained but will not be accessible through the [[traits.OSM]]
    * interface.
    *
    * @param snapshot OSM planet snapshot.
    * @return Typed OSM planet snapshot.
    */
  def asSnapshot(snapshot: DataFrame): Snapshot[Dataset[OSM]] =
    Snapshot(asOSM(snapshot))

  /** Adds type information to a DataFrame containing an OSM planet.
    *
    * Node coordinates are represented as doubles to preserve precision (OSM supports accuracy to 7 decimal places;
    * floats drop to 5 at extreme longitudes). Additional columns will be retained but will not be accessible through
    * the [[traits.OSM]] interface.
    *
    * @param df OSM planet.
    * @return Typed OSM planet.
    */
  def asOSM(df: DataFrame): Dataset[OSM] = {
    import df.sparkSession.implicits._

    implicit val encoder: Encoder[OSM] = buildEncoder[OSM]

    // downcast to reduce storage requirements while preserving sufficient precision
    df.withColumn("type", typeAsByte('type))
      .withColumn("lat", asOptionalDouble('lat))
      .withColumn("lon", asOptionalDouble('lon))
      .withColumn("members", compressMemberTypes('members))
      .withColumn("version", 'version.cast(IntegerType))
      // de-reference
      .withColumn("nds", $"nds.ref")
      .repartition('type, 'id, 'version)
      .as[OSM]
  }
}
