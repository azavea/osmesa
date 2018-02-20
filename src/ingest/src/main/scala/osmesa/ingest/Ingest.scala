package osmesa

import geotrellis.proj4.{LatLng, WebMercator, Transform}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.s3.{S3AttributeStore, S3LayerWriter}
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vectortile._
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{isnull, lit}
import org.apache.spark.sql.types.IntegerType
import vectorpipe._
import vectorpipe.LayerMetadata
import cats.implicits._
import com.monovore.decline._

import java.time.Instant
import spray.json._

import org.geotools.data.DataStore

object Util {
  import spray.json._
  import spray.json.DefaultJsonProtocol._
  import vectorpipe.osm._
  import java.io.ByteArrayInputStream
  import geotrellis.spark.io.s3.S3Client
  import com.amazonaws.services.s3.model.{ObjectMetadata, AmazonS3Exception}

  implicit object ElementMetaWriter extends JsonWriter[ElementMeta] {
    def write(m: ElementMeta): JsValue =
      JsObject(
        "id" -> JsNumber(m.id),
        "user" -> JsString(m.user),
        "uid" -> JsNumber(m.uid),
        "changeset" -> JsNumber(m.changeset),
        "version" -> JsNumber(m.version),
        "timestamp" -> JsString(m.timestamp.toString),
        "visible" -> JsBoolean(m.visible),
        "tags" -> m.tags.toJson
      )
  }

  val s3Client: S3Client = S3Client.DEFAULT

  def logClipFail(e: Extent, f: osm.OSMFeature): Unit = {
    val txt = f.toGeoJson
    val fid = f.data.id
    val gType =
      f.geom match {
        case gc: GeometryCollection => "gc"
        case mp: MultiPolygon => "mp"
        case p: Polygon => "p"
        case ml: MultiLine => "ml"
        case l: Line => "l"
        case mp: MultiPoint => "mp"
        case p: Point => "p"
      }

    val name = s"finland-run1/${gType}-${fid}.json"
    val is = new ByteArrayInputStream(txt.getBytes("UTF-8"))
    s3Client.putObject("vectortiles", s"rde/clip-log/${name}", is, new ObjectMetadata())
  }
}

// object Ingest {
//   def getDataStore(table: String):
// }

/*
 * Usage example:
 *
 * sbt "project ingest" assembly
 *
 * spark-submit --class osmesa.IngestApp ingest/target/scala-2.11/osmesa-ingest.jar --orc=$HOME/data/osm/isle-of-man.orc --bucket=geotrellis-test --key=jpolchlopek/vectortiles --layer=history
 *
 */

object IngestApp extends CommandApp(
  name = "osmesa-ingest",
  header = "Ingest OSM ORC into GeoMesa instance",
  main = {

    /* CLI option handling */
    val orcO = Opts.option[String]("orc", help = "Location of the .orc file to process")
    val bucketO = Opts.option[String]("bucket", help = "S3 bucket to write VTs to")
    val prefixO = Opts.option[String]("key", help = "S3 directory (in bucket) to write to")
    val layerO = Opts.option[String]("layer", help = "Name of the output Layer")
    val maxzoomO = Opts.option[Int]("zoom", help = "Maximum zoom level for ingest (default=14)").withDefault(14)
    val localF = Opts.flag("local", help = "Is this to be run locally, not on EMR?").orFalse
    val pyramidF = Opts.flag("pyramid", help = "Pyramid this layer").orFalse

    (orcO, bucketO, prefixO, layerO, maxzoomO, localF, pyramidF).mapN { (orc, bucket, prefix, layer, maxZoomLevel, local, pyramid) =>

      println(s"ORC: ${orc}")
      println(s"OUTPUT: ${bucket}/${prefix}")
      println(s"LAYER: ${layer}")

      /* Settings compatible for both local and EMR execution */
      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("vp-orc-io")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)

      implicit val ss: SparkSession = SparkSession.builder
        .config(conf)
        .enableHiveSupport
        .getOrCreate

      import ss.implicits._

      /* Silence the damn INFO logger */
      Logger.getRootLogger().setLevel(Level.WARN)

      val df = ss.read.orc(orc)

      val ppnodes = ProcessOSM.preprocessNodes(df)
      val ppways = ProcessOSM.preprocessWays(df)
      val nodeGeoms = ProcessOSM.constructPointGeometries(ppnodes)
      val wayGeoms = ProcessOSM.reconstructWayGeometries(ppnodes, ppways)
      println("PRIOR TO WAY/NODE UNION")
      nodeGeoms.withColumn("minorVersion", lit(null).cast(IntegerType)).printSchema
      wayGeoms.printSchema
      val orderedColumns: List[Column] = List('changeset, 'id, 'version, 'tags, 'geom, 'updated, 'validUntil, 'visible, 'creation, 'authors, 'minorVersion, 'lastAuthor)
      val geoms = wayGeoms
        .select(orderedColumns: _*)
        .union(
          nodeGeoms
            .withColumn("minorVersion", lit(null).cast(IntegerType))
            .select(orderedColumns: _*)
        ).where(!isnull('geom))

      val features: RDD[GenerateVT.VTF[Geometry]] = geoms
        .rdd
        .map { row =>
          val changeset = row.getAs[Long]("changeset")
          val id = row.getAs[Long]("id")
          val version = row.getAs[Long]("version")
          val minorVersion = row.getAs[Int]("minorVersion")
          val tags = row.getAs[Map[String, String]]("tags")
          val geom = row.getAs[scala.Array[Byte]]("geom")
          val updated = row.getAs[java.sql.Timestamp]("updated")
          val creation = row.getAs[java.sql.Timestamp]("creation")
          val authors = row.getAs[Set[String]]("authors").toList.mkString(",")
          val validUntil = row.getAs[java.sql.Timestamp]("validUntil")
          val lastAuthor = row.getAs[String]("lastAuthor")

          // TODO check validity of reprojected geometry + change this to a flatMap so those can be omitted

          Feature(
            geom.readWKB.reproject(LatLng, WebMercator),
            tags.map {
              case (k, v) => (k, VString(v))
            } ++ Map(
              "__changeset" -> VInt64(changeset),
              "__id" -> VInt64(id),
              "__version" -> VInt64(version),
              "__minorVersion" -> VInt64(minorVersion),
              "__updated" -> VInt64(updated.getTime),
              "__validUntil" -> VInt64(Option(validUntil).map(_.getTime).getOrElse(0)),
              "__vtileGen" -> VInt64(new java.sql.Timestamp(System.currentTimeMillis()).getTime),
              "__creation" -> VInt64(creation.getTime),
              "__authors" -> VString(authors),
              "__lastAuthor" -> VString(lastAuthor)
            )
          )
        }

      val layoutScheme = ZoomedLayoutScheme(WebMercator, 512)

      def build[G <: Geometry](keyedGeoms: RDD[(SpatialKey, (SpatialKey, GenerateVT.VTF[G]))], layoutLevel: LayoutLevel): Unit = {
        val LayoutLevel(zoom, layout) = layoutLevel

        GenerateVT.save(GenerateVT.makeVectorTiles(keyedGeoms, layout, layer), zoom, bucket, prefix)

        if (pyramid && zoom > 0)
          build(GenerateVT.upLevel(keyedGeoms), layoutScheme.zoomOut(layoutLevel))
      }

      val maxLayoutLevel = layoutScheme.levelForZoom(maxZoomLevel)
      val keyed = GenerateVT.keyToLayout(features, maxLayoutLevel.layout)

      build(keyed, maxLayoutLevel)

      ss.stop()

      println("Done.")
    }
  }
)
