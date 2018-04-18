package osmesa

import java.net.URI

import cats.implicits._
import cats.data._
import com.monovore.decline._
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vectortile._
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import osmesa.ingest.util.Caching
import vectorpipe._

import scala.collection.mutable.ArrayBuffer

object Util {
  import java.io.ByteArrayInputStream

  import com.amazonaws.services.s3.model.ObjectMetadata
  import geotrellis.spark.io.s3.S3Client
  import spray.json.DefaultJsonProtocol._
  import spray.json._
  import vectorpipe.osm._

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
 * spark-submit \
 *   --class osmesa.Ingest \
 *   ingest/target/scala-2.11/osmesa-ingest.jar \
 *   --orc=$HOME/data/osm/isle-of-man.orc \
 *   --changesets=$HOME/data/isle-of-man-changesets.orc \
 *   --bucket=geotrellis-test \
 *   --key=jpolchlopek/vectortiles \
 *   --layer=history
 *
 */

object Ingest extends CommandApp(
  name = "osmesa-ingest",
  header = "Ingest OSM ORC into GeoMesa instance",
  main = {

    // Used for producing an extent from the provided string
    implicit val readExtent: Argument[Extent] = new Argument[Extent] {

      override def read(string: String): ValidatedNel[String, Extent] =
        try { Validated.valid(Extent.fromString(string)) }
        catch { case _: Exception => Validated.invalidNel(s"Invalid Extent: unable to parse $string") }

      override def defaultMetavar: String = "extent"
    }
    /* CLI option handling */
    val orcO = Opts.option[String]("orc", help = "Location of the ORC file to process")
    val changesetsO = Opts.option[String]("changesets", help = "Location of the ORC file containing changesets")
    val bucketO = Opts.option[String]("bucket", help = "S3 bucket to write VTs to")
    val prefixO = Opts.option[String]("key", help = "S3 directory (in bucket) to write to")
    val layerO = Opts.option[String]("layer", help = "Name of the output Layer")
    val maxzoomO = Opts.option[Int]("zoom", help = "Maximum zoom level for ingest (default=14)").withDefault(14)
    val pyramidF = Opts.flag("pyramid", help = "Pyramid this layer").orFalse
    val cacheDirO = Opts.option[String]("cache", help = "Location to cache ORC files").withDefault("")
    val extentO = Opts.option[Extent]("extent", help = "Geographic extent for this layer").orNone

    (orcO, changesetsO, bucketO, prefixO, layerO, maxzoomO, pyramidF, cacheDirO, extentO).mapN { (orc, changesetsSrc, bucket, prefix, layer, maxZoomLevel, pyramid, cacheDir, extent) =>

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
      Logger.getRootLogger.setLevel(Level.WARN)

      val df = ss.read.orc(orc)
      val changesets = ss.read.orc(changesetsSrc)

      val cache = Option(new URI(cacheDir).getScheme) match {
        case Some("s3") => Caching.onS3(cacheDir)
        // bare paths don't get a scheme
        case None if cacheDir != "" => Caching.onFs(cacheDir)
        case _ => Caching.none
      }

      val ppnodes = cache.orc("prepared_nodes.orc") {
        ProcessOSM.preprocessNodes(df, extent)
      }

      val ppways = cache.orc("prepared_ways.orc") {
        ProcessOSM.preprocessWays(df)
      }

      val nodeGeoms = cache.orc("node_geoms.orc") {
        ProcessOSM.constructPointGeometries(ppnodes)
          .withColumn("minorVersion", lit(0))
          .join(changesets.select('id as 'changeset, 'user), Array("changeset"))
      }

      val wayGeoms = cache.orc("way_geoms.orc") {
        ProcessOSM.reconstructWayGeometries(ppways, ppnodes)
          .join(changesets.select('id as 'changeset, 'user), Array("changeset"))
      }

      val nodeAugmentations = nodeGeoms
        .join(changesets.select('id as 'changeset, 'uid, 'user as 'author), Array("changeset"))
        .groupBy('id)
        .agg(min('updated) as 'creation, collect_set('author) as 'authors)

      val wayAugmentations = wayGeoms
        .join(changesets.select('id as 'changeset, 'uid, 'user as 'author), Array("changeset"))
        .groupBy('id)
        .agg(min('updated) as 'creation, collect_set('author) as 'authors)

      val latestNodeGeoms = ProcessOSM.snapshot(nodeGeoms)
        .join(nodeAugmentations, Array("id"))
      val latestWayGeoms = ProcessOSM.snapshot(wayGeoms)
        .join(wayAugmentations, Array("id"))

      println("PRIOR TO WAY/NODE UNION")
      latestNodeGeoms.printSchema
      latestWayGeoms.printSchema

      val geoms = cache.orc(s"computed_geoms_z${maxZoomLevel}.orc") {
        latestWayGeoms
          .union(latestNodeGeoms)
          .where('geom.isNotNull)
      }

      val features: RDD[GenerateVT.VTF[Geometry]] = geoms
        .rdd
        .flatMap { row =>
          val changeset = row.getAs[Long]("changeset")
          val id = row.getAs[Long]("id")
          val version = row.getAs[Long]("version")
          val minorVersion = row.getAs[Int]("minorVersion")
          val tags = row.getAs[Map[String, String]]("tags")
          val geom = row.getAs[scala.Array[Byte]]("geom")
          val updated = row.getAs[java.sql.Timestamp]("updated")
          val creation = row.getAs[java.sql.Timestamp]("creation")
          val authors = row.getAs[Set[String]]("authors").toArray.mkString(",")
          val user = row.getAs[String]("user")
          val _type = row.getAs[Byte]("_type")

          // e.g. n123, w456, r789
          val elementId = _type match {
            case ProcessOSM.NODE_TYPE => s"n${id}"
            case ProcessOSM.WAY_TYPE => s"w${id}"
            case ProcessOSM.RELATION_TYPE => s"r${id}"
          }

          // check validity of reprojected geometry
          geom.readWKB.reproject(LatLng, WebMercator) match {
            case g if g.isValid =>
              ArrayBuffer(Feature(
                g,
                tags.map {
                  case (k, v) => (k, VString(v))
                } ++ Map(
                  "__changeset" -> VInt64(changeset),
                  "__id" -> VString(elementId),
                  "__version" -> VInt64(version),
                  "__minorVersion" -> VInt64(minorVersion),
                  "__updated" -> VInt64(updated.getTime),
                  "__vtileGen" -> VInt64(new java.sql.Timestamp(System.currentTimeMillis()).getTime),
                  "__creation" -> VInt64(creation.getTime),
                  "__authors" -> VString(authors),
                  "__lastAuthor" -> VString(user)
                )
              ))
            case _ => ArrayBuffer()
          }
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
