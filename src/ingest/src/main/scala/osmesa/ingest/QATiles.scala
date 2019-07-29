package osmesa.ingest

import cats.implicits._
import com.monovore.decline._
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.spark.io.kryo.KryoRegistrator
import geotrellis.spark.io.s3.util.S3RangeReader
import geotrellis.util.FileRangeReader
import geotrellis.vector.io.json.{GeoJson, JsonFeatureCollection}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{array_contains, isnull, not, when}
import org.locationtech.geomesa.spark.jts._
import org.locationtech.jts.geom._
import vectorpipe._
import vectorpipe.functions.osm._
import vectorpipe.vectortile._

import java.net.URI

case class QATilesPipeline(geometryColumn: String, baseOutputURI: URI) extends Pipeline {
  val layerMultiplicity = LayerNamesInColumn("layers")

  override def simplify(geom: Geometry, layout: LayoutDefinition): Geometry = Simplify.withJTS(geom, layout)

  override def clip(geom: Geometry, key: geotrellis.spark.SpatialKey, layoutLevel: geotrellis.spark.tiling.LayoutLevel): Geometry =
    Clipping.byLayoutCell(geom, key, layoutLevel)
}

object QATiles extends CommandApp(
  name="qa-tile-generator",
  header="Create QA tiles from OSM ORC file",
  main = {
    /* CLI option handling */
    val minZoomOpt = Opts.option[Int]("minZoom", help = "Smallest (least resolute) zoom level to generate").withDefault(0)
    val maxZoomOpt = Opts.option[Int]("maxZoom", help = "Largest (most resolute) zoom level to generate").withDefault(15)
    val aoiOpt = Opts.option[URI]("aois", help = "Location of GeoJSON file giving areas of interest").withDefault(new URI(""))
      .validate("AOI file must be a .geojson file") { uri => uri.toString.isEmpty || uri.toString.endsWith(".geojson") }
    val orcArg = Opts
      .argument[URI]("source ORC file")//, help = "Location of the ORC file containing geometries to process")
      .validate("URI to ORC must have an s3 or file scheme") { _.getScheme != null }
      .validate("orc must be an S3 or file Uri") { uri =>
        uri.getScheme.startsWith("s3") || uri.getScheme.startsWith("file")
      }
      .validate("orc must be an .orc file") { _.getPath.endsWith(".orc") }
    val outputArg = Opts.argument[URI]("output URI")//, help = "URI giving S3 location for output tiles")
      .validate("Output URI must have a scheme") { _.getScheme != null }
      .validate("Output URI must have an S3 or file scheme") { uri =>
        uri.getScheme.startsWith("s3") || uri.getScheme.startsWith("file")
      }

    (minZoomOpt, maxZoomOpt, aoiOpt, orcArg, outputArg).mapN { (minZoom, maxZoom, aoiUri, orcUri, outputUri) =>

      val aoiGeoms =
        if (!aoiUri.toString.isEmpty) {
          val geojsonString =
            if (aoiUri.getScheme == "s3")
              new String(S3RangeReader(aoiUri).readAll)
            else
              new String(FileRangeReader(aoiUri.toString).readAll)
          println(s"Narrowing to area of interest containing\n${geojsonString}")
          Some(GeoJson.parse[JsonFeatureCollection](geojsonString).getAllGeometries.toList)
        } else
          None

      val conf =
        new SparkConf()
          .setAppName("QA Tile Generator")
          .set("spark.serializer", classOf[KryoSerializer].getName)
          .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
          .set("spark.sql.broadcastTimeout", "600")
          .set("spark.kryoserializer.buffer.max", "1g")

      implicit val spark =
        SparkSession.builder
          .config(conf)
          .getOrCreate
          .withJTS

      import spark.implicits._

      val valid_geom = functions.udf { g: Geometry => !g.isEmpty && g.isValid}
      val within_aoi = functions.udf { g: Geometry => aoiGeoms.map(_.exists(_.intersects(g))).getOrElse(true) }

      val features = OSM
        .toGeometry(ensureCompressedMembers(spark.read.orc(orcUri.toString)))
        .where(not(isnull('geom)) and isnull('validUntil) and valid_geom('geom))
        .where(within_aoi('geom))
        .filter(not(QAFunctions.st_emptyGeom('geom)))
        .withColumn("layers", when(isBuilding('tags), "buildings")
                              .when(isRoad('tags), "roads")
                              .when(QAFunctions.isForest('tags), "forests")
                              .when(QAFunctions.isLake('tags), "lakes")
                              .when(QAFunctions.isRiver('tags), "rivers"))
        .where(not(isnull('layers)))

      val pipeline = QATilesPipeline("geom", outputUri)

      VectorPipe(features, pipeline, VectorPipe.Options.forZoomRange(minZoom, maxZoom))
    }
  }
)

object QAFunctions {
  val st_emptyGeom = functions.udf { g: Geometry => g.isEmpty }

  def isForest(tags: Column): Column =
    array_contains(splitDelimitedValues(tags.getItem("landuse")), "forest") as 'isForest

  def isLake(tags: Column): Column =
    (array_contains(splitDelimitedValues(tags.getItem("natural")), "water") and
     (tags("water").isNull or
      not(array_contains(splitDelimitedValues(tags.getItem("water")), "river") or
          array_contains(splitDelimitedValues(tags.getItem("water")), "canal")
         )
     )
    ) as 'isLake

  def isRiver(tags: Column): Column =
    array_contains(splitDelimitedValues(tags.getItem("waterway")), "river") as 'isRiver
}
