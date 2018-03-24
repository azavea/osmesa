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

  def relation(relation: Int): (Fixture) = Fixture(orc(s"relation-$relation.orc"), wkt(s"relation-$relation.wkt"))

  def orc(filename: String): DataFrame = spark.read.orc(getClass.getResource("/" + filename).getPath)

  def wkt(filename: String): Seq[String] = {
    try {
      Source.fromInputStream(getClass.getResourceAsStream("/" + filename)).getLines.toSeq
    } catch {
      case _: Exception => Seq()
    }
  }

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
// https://www.openstreetmap.org/relation/64420
// to find multipolygons: select osm_id from planet_osm_polygon where osm_id < 0 and ST_GeometryType(way) = 'ST_MultiPolygon' order by osm_id desc;
class MultiPolygonRelationExamples extends SparkPoweredTables {
  def examples = Table("multipolygon relation",
    relation(333501), // unordered, single polygon with 1 hole
    relation(393502), // single polygon, multiple outer parts, no holes
    relation(1949938), // unordered, single polygon with multiple holes
    relation(3105056), // multiple unordered outer parts in varying directions
    relation(2580685) // multipolygon: 2 polygons, one with 1 hole
  )
}

class MultiPolygonRelationReconstructionSpec extends PropSpec with TableDrivenPropertyChecks with Matchers {
  // TODO 1280388@v1 for an old-style multipolygon (tags on ways)
  property("should match expected WKT") {
    new MultiPolygonRelationExamples {
      forAll(examples) { fixture =>
        val actual = asWKT(ProcessOSM.reconstructRelationGeometries(fixture.members))
        val expected = fixture.wkt

        println("Actual:")
        actual.foreach(println)
        println("Expected:")
        expected.foreach(println)

        actual should === (expected)
      }
    }
  }
}
