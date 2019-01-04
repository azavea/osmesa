package common

import java.sql.Timestamp

import geotrellis.spark.io.kryo.KryoRegistrator
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.jts._
import org.scalatest.prop.{TableDrivenPropertyChecks, Tables}
import org.scalatest.{Matchers, PropSpec}
import osmesa.common.ProcessOSM._
import osmesa.common.functions._
import osmesa.common.functions.osm._
import com.vividsolutions.jts.{geom => jts}
import com.vividsolutions.jts.io.WKTReader
import org.locationtech.geomesa.spark.jts._
import osmesa.common.relations.MultiPolygons.build

import scala.io.Source

case class Fixture(id: Int, members: DataFrame, wkt: Seq[String])

trait SparkPoweredTables extends Tables {
  def wktReader = new WKTReader()

  val spark: SparkSession = SparkSession
    .builder
    .config(
      /* Settings compatible with both local and EMR execution */
      new SparkConf()
        .setAppName(getClass.getName)
        .setIfMissing("spark.master", "local[*]")
        .setIfMissing("spark.serializer", classOf[KryoSerializer].getName)
        .setIfMissing("spark.kryo.registrator", classOf[KryoRegistrator].getName)
    ).getOrCreate()
  spark.withJTS

  def relation(relation: Int): Fixture = Fixture(relation, orc(s"relation-$relation.orc"), readWktFile(s"relation-$relation.wkt"))

  def orc(filename: String): DataFrame = spark.read.orc(getClass.getResource("/" + filename).getPath)

// osm2pgsql -c -d rhode_island -j -K -l rhode-island-latest.osm.pbf
// select ST_AsText(way) from planet_osm_polygon where osm_id=-333501;

  def readWktFile(filename: String): Seq[String] =
    try {
      Source.fromInputStream(getClass.getResourceAsStream("/" + filename)).getLines.toSeq match {
        case expected if expected.isEmpty =>
          Seq()
        case expected =>
          expected
      }
    } catch {
      case _: Exception => Seq("[not provided]")
    }

  def asGeoms(relations: DataFrame): Seq[jts.Geometry] = {
    import relations.sparkSession.implicits._

    relations.select('geom).collect.map { row =>
      row.getAs[jts.Geometry]("geom")
    }
  }
}

// osm2pgsql -c -d rhode_island -j -K -l rhode-island-latest.osm.pbf
// select ST_AsText(way) from planet_osm_polygon where osm_id=-333501;
// to debug / visually validate (geoms won't match exactly), load WKT into geojson.io from Meta → Load WKT String
// https://www.openstreetmap.org/relation/64420
// to find multipolygons: select osm_id from planet_osm_polygon where osm_id < 0 and ST_GeometryType(way) = 'ST_MultiPolygon' order by osm_id desc;
class MultiPolygonRelationExamples extends SparkPoweredTables {
  def examples = Table("multipolygon relation",
    relation(333501), // unordered, single polygon with 1 hole
    relation(393502), // single polygon, multiple outer parts, no holes
    relation(1949938), // unordered, single polygon with multiple holes
    relation(3105056), // multiple unordered outer parts in varying directions
    relation(2580685), // multipolygon: 2 polygons, one with 1 hole
    relation(3080946), // multipolygon: many polygons, no holes
    relation(5448156), // multipolygon made up of parcels
    relation(5448691), // multipolygon made up of parcels
    relation(6710544), // complex multipolygon
    relation(191199), // 4 segments; 2 are components of another (thus duplicates)
    relation(61315), // incomplete member list (sourced from an extract of a neighboring state)
    relation(2554903), // boundary w/ admin_centre + label node members
    relation(191204), // no members
    relation(110564), // touching but not dissolve-able
    relation(5612959) // pathological case for unioning
  )
}

class MultiPolygonRelationReconstructionSpec extends PropSpec with TableDrivenPropertyChecks with Matchers {
  property("should match expected WKT") {
    new MultiPolygonRelationExamples {
      forAll(examples) { fixture =>
        import fixture.members.sparkSession.implicits._

        implicit val encoder: Encoder[Row] = VersionedElementEncoder

        // TODO rewrite fixtures with additional columns added below
        val actual: Seq[jts.Geometry] = asGeoms(fixture.members
          .withColumn("version", lit(1))
          .withColumn("minorVersion", lit(0))
          .withColumn("updated", lit(Timestamp.valueOf("2001-01-01 00:00:00")))
          .withColumn("validUntil", lit(Timestamp.valueOf("2002-01-01 00:00:00")))
          .withColumn("geometry", st_geomFromWKB('geom))
          .groupByKey { row =>
            (row.getAs[Long]("changeset"), row.getAs[Long]("id"), row.getAs[Integer]("version"), row.getAs[Integer]
              ("minorVersion"), row.getAs[Timestamp]("updated"), row.getAs[Timestamp]("validUntil"))
          }
          .mapGroups {
            case ((changeset, id, version, minorVersion, updated, validUntil), rows) =>
              val members = rows.toVector
              // TODO store Bytes as the type in fixtures
              val types = members.map(_.getAs[String]("type") match {
                case "node" => NodeType
                case "way" => WayType
                case "relation" => RelationType
                case _ => null.asInstanceOf[Byte]
              })
              val roles = members.map(_.getAs[String]("role"))
              //val geoms = members.map(_.getAs[jts.Geometry]("geom"))
              val geoms = members.map(_.getAs[jts.Geometry]("geometry"))
              val mp = build(id, version, updated, types, roles, geoms).orNull

              new GenericRowWithSchema(Array(changeset, id, version, minorVersion, updated, validUntil, mp),
                VersionedElementSchema): Row
          }
        ).map(Option.apply).flatten

        val expected = fixture.wkt.map(wktReader.read)

        try {
          actual should ===(expected)
        } catch {
          case e: Throwable =>
            println(s"${fixture.id} actual:")
            actual.foreach(println)
            println(s"${fixture.id} expected:")
            fixture.wkt.foreach(println)

            throw e
        }
      }
    }
  }
}
