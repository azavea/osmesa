package osmesa.ingest

import cats.effect.{IO, Timer}
import cats.syntax.applicative._
import cats.syntax.apply._
import cats.syntax.either._
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest, PutObjectResult}
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.monovore.decline._
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.{IntArrayTile, Raster, RasterExtent}
import geotrellis.raster.render._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.spark.{SpatialKey}
import geotrellis.spark.tiling.{LayoutDefinition, ZoomedLayoutScheme}
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.kryo.KryoRegistrator
import geotrellis.spark.io.s3._
import geotrellis.spark.io.s3.util.S3RangeReader
import geotrellis.spark.io.s3.conf.S3Config
import geotrellis.util.FileRangeReader
import geotrellis.vector
import io.circe._
import io.circe.parser._
import io.circe.syntax._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.functions.{array_contains, coalesce, col, isnull, lit, lower, not, when}
import org.locationtech.geomesa.spark.jts._
import org.locationtech.jts.geom._
import vectorpipe._
import vectorpipe.functions.osm._
import vectorpipe.vectortile._

import java.io.ByteArrayInputStream
import java.net.URI
import java.util.concurrent.Executors
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

object RasterizeOSM extends CommandApp(
  name="qa-tile-generator",
  header=""" Create QA tiles from OSM ORC file
           |
           | Utility that allows for generating rasterized tiles from OSM data.  One may
           | colorize according to a rule set defined in a JSON file.  The structure of
           | those files is as follows:
           |
           | [
           |  {
           |   "condition": ["key=value,key=value", "key=value"],
           |   "color": "#0000ff"
           |  },
           |  {
           |   "condition": ["key", "key=value", "key,key=value"],
           |   "subtree": [
           |    {
           |     "condition": ["key=value"],
           |     "color": "#00ff00"
           |    },
           |    {
           |     "condition": [],
           |     "color": "#0f0f0f"
           |    }
           |   ]
           |  },
           |  {
           |   "condition": ["key=value", "key=value,key=value"],
           |   "color": "#000000"
           |  },
           |  {
           |   "condition": [],
           |   "color": "#ffffff"
           |  }
           | ]
           |
           | Each entry in this array is an object containing an array of conditions.  The
           | condition array comprises comma-delimited tests of two forms:
           |
           |   1. "key=value" checks: Return true when the key exists and has the given
           |      value
           |   2. "key" checks: Returns true when the key exists and does not attain one
           |      of the false-like values "no", "false", or "0"
           |
           | Every element of the condition array must have one test that evaluates to true
           | for the condition to be satisfied.  So
           |              ["building", "roof:material=wood,building:roof=wood"]
           | will be true for an entity with tag "building" not having a false value and
           | when either "roof:material" or "building:roof" exists and attains the value
           | "wood".  An empty array condition evaluates to true always.
           |
           | If a condition is satisfied and the block has a "color" field, then the pixels
           | corresponding to that element will be assigned the given color value.  If the
           | block instead has a "subtree" field, the described process will be applied
           | recursively until a color has been assigned.
           |
           | If no color gets assigned in this process, the element will not be rasterized.
           |
           | If this JSON file is not supplied, all geometries will be rasterized in black
           | on a transparent background.
         """.stripMargin,
  main = {
    /* CLI option handling */
    val aoiFileOpt = Opts.option[URI]("aoiFile", help = "Location of GeoJSON file giving areas of interest").withDefault(new URI(""))
      .validate("AOI file must be a .geojson file") { uri => uri.toString.isEmpty || uri.toString.endsWith(".geojson") }
    val colorFileOpt = Opts.option[URI]("colorFile", help = "Location of JSON file describing how to colorize entities").withDefault(new URI(""))
      .validate("color file must be a .json file") { uri => uri.toString.isEmpty || uri.toString.endsWith(".json") }
    val minZoomOpt = Opts.option[Int]("minZoom", help = "Smallest (least resolute) zoom level to generate").withDefault(0)
    val maxZoomOpt = Opts.option[Int]("maxZoom", help = "Largest (most resolute) zoom level to generate").withDefault(15)
    val tileResOpt = Opts.option[Int]("resolution", help = "Resolution of output tile in pixels").withDefault(256)

    val orcArg = Opts
      .argument[URI]("source ORC file")
      .validate("URI to ORC must have an s3 or file scheme") { _.getScheme != null }
      .validate("orc must be an S3 or file Uri") { uri =>
        uri.getScheme.startsWith("s3") || uri.getScheme.startsWith("file")
      }
      .validate("orc must be an .orc file") { _.getPath.endsWith(".orc") }
    val outputArg = Opts.argument[URI]("output URI")
      .validate("Output URI must have a scheme") { _.getScheme != null }
      .validate("Output URI must have an S3 or file scheme") { uri =>
        uri.getScheme.startsWith("s3") || uri.getScheme.startsWith("file")
      }

    (aoiFileOpt, colorFileOpt, minZoomOpt, maxZoomOpt, tileResOpt, orcArg, outputArg).mapN { case (aoiUri, colorUri, minZoom, maxZoom, tileRes, orcUri, outputUri) =>

      import OSMRasterizer._

      val colorJsonString =
        if (colorUri.toString.length > 0) {
          if (colorUri.getScheme == "s3")
            new String(S3RangeReader(colorUri).readAll)
          else
            new String(FileRangeReader(colorUri.toString).readAll)
        } else {
          """[
            |  {
            |    "condition": [],
            |    "color": "#000000"
            |  }
            |]
          """.stripMargin
        }

      val colorRules =
        (for {
          ast <- parse(colorJsonString)
          nodes <- ast.as[Seq[ColorTreeNode]]
        } yield nodes) match {
          case Right(seq) => seq
          case Left(err) => throw new IllegalArgumentException(err.toString)
        }

      val colorToLevel = colorRules.flatMap(_.colors).toSet.toSeq.zipWithIndex.toMap
      val colorMap = ColorMap(colorToLevel.map{ case (c, v) => (v, BigInt(c.drop(1).padTo(8,'f')).toInt) })

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

      val features = OSM
        .toGeometry(ensureCompressedMembers(spark.read.orc(orcUri.toString)))
        .where(not(isnull('geom)) and isnull('validUntil))
        .withColumn("color", coalesce( (colorRules.map(_.eval('tags))): _* ))
        .where(not(isnull('color)))
        .as[GeometryWithColor]
        .map(_.reproject)

      val zls = ZoomedLayoutScheme(WebMercator)

      for {
        z <- Range(minZoom, maxZoom).inclusive
      } {
        val layout = zls.levelForZoom(z).layout
        val keyed = features.flatMap(_.keyAndDiscretize(layout, colorToLevel)).groupByKey(_.key)

        // TODO: throw away geometry outside the AOI

        val rasterized = keyed.mapGroups{ case (key, iter) => {
          val raster = Raster(IntArrayTile.empty(tileRes, tileRes), layout.mapTransform.keyToExtent(key))

          iter.foreach { rlk =>
            Rasterizer.foreachCellByGeometry(rlk.reprojected, raster.rasterExtent){ (c, r) => raster.tile.set(c, r, rlk.level) }
          }

          KeyedPNG(key, raster.tile.renderPng(colorMap).bytes)
        }}

        def pngToLocation(kpng: KeyedPNG): String = {
          val SpatialKey(x, y) = kpng.key
          val sep = if (outputUri.toString.last == '/') "" else "/"
          s"${outputUri}${sep}${z}/${x}/${y}.png"
        }

        outputUri.getScheme match {
          case "s3" =>
            saveToS3[KeyedPNG](rasterized, pngToLocation(_), _.png)
          case "file" =>
            saveToHadoop[KeyedPNG](rasterized, pngToLocation(_), _.png)
        }
      }
    }
  }
)

object OSMRasterizer {
  case class GeometryWithColor(geom: Geometry, color: String) {
    def reproject: ReprojectedWithColor = {
      val reprojected = vector.reproject.Reproject(vector.Geometry(geom), LatLng, WebMercator).jtsGeom
      ReprojectedWithColor(reprojected, color)
    }
  }
  case class ReprojectedWithColor(reprojected: Geometry, color: String) {
    def keyAndDiscretize(layout: LayoutDefinition, colorChart: Map[String, Int]): Seq[ReprojectedWithLevelAndKey] = {
      layout.mapTransform.keysForGeometry(vector.Geometry(reprojected)).toSeq.map { key => ReprojectedWithLevelAndKey(reprojected, colorChart(color), key) }
    }
  }
  case class ReprojectedWithLevelAndKey(reprojected: Geometry, level: Int, key: SpatialKey)
  case class KeyedPNG(key: SpatialKey, png: Array[Byte])

  trait Condition {
    def key: String
    def eval(tags: Column): Column
  }
  case class ExistenceCondition(key: String) extends Condition {
    def eval(tags: Column): Column =
      !lower(coalesce(tags.getItem(key), lit("no"))).isin(Seq("no", "false", "0"): _*) as (s"eval(${key})")
  }
  case class EqualityCondition(key: String, value: String) extends Condition {
    def eval(tags: Column): Column =
      array_contains(splitDelimitedValues(tags.getItem(key)), value) as (s"eval(${key}=${value})")
  }

  def parseConditions(conditions: String): Seq[Condition] = {
    def convertToCondition(condition: String): Condition = {
      condition.split("=").toSeq.map(_.trim) match {
        case Seq(key) => ExistenceCondition(key)
        case Seq(key, value) => EqualityCondition(key, value)
        case _ => throw new IllegalArgumentException(s"""Cannot parse condition string: "$condition" """)
      }
    }

    conditions.split(",").map(_.trim).map(convertToCondition(_))
  }

  def evaluateAllConditions(conditions: Seq[Seq[Condition]], tags: Column): Column = {
    def disjunction(conds: Seq[Column]): Column = {
      conds match {
        case Seq() => lit(true)
        case Seq(cond) => cond
        case s => s.head or disjunction(s.tail)
      }
    }
    def conjunction(conds: Seq[Column]): Column = {
      conds match {
        case Seq() => lit(true)
        case Seq(cond) => cond
        case s => s.head and disjunction(s.tail)
      }
    }

    conjunction(conditions.map{ conds => disjunction(conds.map(_.eval(tags))) })
  }

  trait ColorTreeNode {
    def conditions: Seq[Seq[Condition]]
    def eval(tags: Column): Column
    def colors: Seq[String]
  }
  case class Leaf(conditions: Seq[Seq[Condition]], color: String) extends ColorTreeNode {
    assert(color(0)=='#' && (color.length==7 || color.length==9), s"Colors must be of the form '#123456'; received $color")

    def eval(tags: Column): Column = when(evaluateAllConditions(conditions, tags), lit(color))
    def colors = Seq(color)
  }
  case class Interior(conditions: Seq[Seq[Condition]], subtree: Seq[ColorTreeNode]) extends ColorTreeNode {
    assert(subtree.length > 0, "Expression subtree cannot be empty!")

    def eval(tags: Column): Column = {
      val subevals = subtree.map(_.eval(tags))
      when(evaluateAllConditions(conditions, tags), coalesce(subevals: _*))
    }

    def colors = subtree.flatMap(_.colors)
  }

  implicit val colorTreeDecoder: Decoder[ColorTreeNode] = new Decoder[ColorTreeNode] {
    final def apply(c: HCursor): Decoder.Result[ColorTreeNode] = {
      if (c.downField("color").succeeded) {
        for {
          conditions <- c.downField("condition").as[Seq[String]].map(_.map(parseConditions(_)))
          color <- c.downField("color").as[String]
        } yield Leaf(conditions, color)
      } else {
        for {
          conditions <- c.downField("condition").as[Seq[String]].map(_.map(parseConditions(_)))
          subtree <- c.downField("subtree").as[Seq[ColorTreeNode]]
        } yield Interior(conditions, subtree)
      }
    }
  }

  def saveToS3[K](
    dataset: Dataset[K],
    toUri: K => String,
    toBytes: K => Array[Byte],
    putObjectModifier: PutObjectRequest => PutObjectRequest = {p => p},
    s3Maker: () => S3Client = () => S3Client.DEFAULT,
    threads: Int = S3Config.threads.rdd.writeThreads
  ): Unit = {
    val toPrefix: K => (String, String) = value => {
      val uri = new URI(toUri(value))
      require(uri.getScheme == "s3", s"saveToS3 only supports s3 scheme: $uri")
      val bucket = uri.getAuthority
      val prefix = uri.getPath.substring(1) // drop the leading / from the prefix
      (bucket, prefix)
    }

    dataset.foreachPartition { partition =>
      val s3Client = s3Maker()
      val requests: fs2.Stream[IO, PutObjectRequest] =
        fs2.Stream.fromIterator[IO, PutObjectRequest](
          partition.map { value =>
            val bytes = toBytes(value)
            val metadata = new ObjectMetadata()
            metadata.setContentLength(bytes.length)
            val is = new ByteArrayInputStream(bytes)
            val (bucket, path) = toPrefix(value)
            putObjectModifier(new PutObjectRequest(bucket, path, is, metadata))
          }
        )

      val pool = Executors.newFixedThreadPool(threads)
      implicit val ec = ExecutionContext.fromExecutor(pool)
      implicit val timer: Timer[IO] = IO.timer(ec)
      implicit val cs = IO.contextShift(ec)

      import geotrellis.spark.util.TaskUtils._
      val write: PutObjectRequest => fs2.Stream[IO, PutObjectResult] = { request =>
        fs2.Stream eval IO.shift(ec) *> IO {
          request.getInputStream.reset() // reset in case of retransmission to avoid 400 error
          s3Client.putObject(request)
        }.retryEBO {
          case e: AmazonS3Exception if e.getStatusCode == 503 => true
          case _ => false
        }
      }

      requests
        .map(write)
        .parJoin(threads)
        .compile
        .toVector
        .unsafeRunSync()
      pool.shutdown()
    }
  }

  def saveToHadoop[K](
    dataset: Dataset[K],
    toUri: K => String,
    toBytes: K => Array[Byte]
  ): Unit = {
    val conf = dataset.sparkSession.sparkContext.hadoopConfiguration

    def saveIterator(recs: Iterator[K]): Unit = {
      val fsCache = TrieMap.empty[String, FileSystem]

      recs.foreach { value =>
        val path = toUri(value)
        val uri = new URI(path)
        val fs = fsCache.getOrElseUpdate(
          uri.getScheme,
          FileSystem.get(uri, conf))
        val out = fs.create(new Path(path))
        try {
          out.write(toBytes(value))
        } finally {
          out.close()
        }
      }
    }

    dataset.foreachPartition(saveIterator(_))
  }
}
