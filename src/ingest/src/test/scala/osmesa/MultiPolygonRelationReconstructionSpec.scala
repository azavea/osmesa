package osmesa

import geotrellis.spark.io.kryo.KryoRegistrator
import geotrellis.vector.io._
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.prop.{TableDrivenPropertyChecks, Tables}
import org.scalatest.{Matchers, PropSpec}

import scala.io.Source

case class Fixture(members: DataFrame, wkt: Seq[String])

trait SparkPoweredTables extends Tables {
  val spark: SparkSession = SparkSession
    .builder
    .config(
      /* Settings compatible with both local and EMR execution */
      new SparkConf()
        .setAppName(getClass.getName)
        .setIfMissing("spark.master", "local[*]")
        .setIfMissing("spark.serializer", classOf[KryoSerializer].getName)
        .setIfMissing("spark.kryo.registrator", classOf[KryoRegistrator].getName)
    )
    .getOrCreate()

  def fixture(relation: Int): (Fixture) = Fixture(orc(s"relation-$relation.orc"), wkt(s"relation-$relation.wkt"))

  def orc(filename: String): DataFrame = spark.read.orc(getClass.getResource("/" + filename).getPath)

  def wkt(filename: String): Seq[String] = Source.fromInputStream(getClass.getResourceAsStream("/" + filename)).getLines.toSeq

  private val _asWKT: UserDefinedFunction = udf((geom: Array[Byte]) => {
    geom match {
      case null => ""
      case _ => geom.readWKB.toWKT
    }
  })

  def asWKT(relations: DataFrame): Seq[String] = {
    import relations.sparkSession.implicits._

    relations.select(_asWKT('geom).as("wkt")).collect.map { row =>
      row.getAs[String]("wkt")
    }
  }
}

// osm2pgsql -c -d rhode_island -j -K -l rhode-island-latest.osm.pbf
// select ST_AsText(way) from planet_osm_polygon where osm_id=-333501;
// to debug / visually validate (geoms won't match exactly), load WKT into geojson.io from Meta → Load WKT String
class MultiPolygonRelationExamples extends SparkPoweredTables {
  def examples = Table("multipolygon relation", fixture(333501), fixture(393502))
}

class MultiPolygonRelationReconstructionSpec extends PropSpec with TableDrivenPropertyChecks with Matchers {
  property("should match osm2pgsql WKT") {
    new MultiPolygonRelationExamples {
      forAll(examples) { fixture =>
        val actual = asWKT(ProcessOSM.reconstructRelationGeometries(fixture.members))
        val expected = fixture.wkt

        println(actual(0))
        println(expected(0))

        actual should === (expected)
      }
    }
  }
}
