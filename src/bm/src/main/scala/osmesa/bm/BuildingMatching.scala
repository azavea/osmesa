package osmesa.bm

import geotrellis.proj4.{LatLng, WebMercator, Transform}
import geotrellis.spark._
import geotrellis.spark.join._
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vectortile._

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import vectorpipe._
import vectorpipe.osm._

import cats.implicits._
import com.monovore.decline._
import monocle.macros.GenLens

import osmesa.GenerateVT

import com.vividsolutions.jts.algorithm.Centroid

import scala.collection.mutable


object Util {

  def sparkSession(appName: String): SparkSession = {
    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setAppName(s"OSMesa Analytics - ${appName}")
      .set("spark.sql.orc.filterPushdown", "true")
      .set("spark.hadoop.parquet.enable.summary-metadata", "false")
      .set("spark.sql.parquet.mergeSchema", "false")
      .set("spark.sql.parquet.filterPushdown", "true")
      .set("spark.sql.hive.metastorePartitionPruning", "true")

    SparkSession.builder
      .config(conf)
      .enableHiveSupport
      .getOrCreate
  }

  def filterfn1(clipGeometry: Option[Geometry])(feature: OSMFeature): Boolean = {
    clipGeometry match {
      case Some(g) => feature.geom.intersects(g)
      case None => true
    }
  }

  def filterfn2(clipGeometry: Option[Geometry])(pair: (Long, Node)): Boolean = {
    clipGeometry match {
      case Some(g) =>
        val node = pair._2
        Point(node.lon, node.lat).intersects(g)
      case None => true
    }
  }

}

object BuildingMatching extends CommandApp(
  name = "Building-Matching",
  header = "Match Buildings",
  main = {
    val ss = Util.sparkSession("Building-Matching")
    val tags = GenLens[vectorpipe.osm.ElementMeta](_.tags)

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val clipGeometry =
      Opts.option[String]("clipGeometry", help = "GeoJSON file containing clip geometry").orNone
    val dataset1 =
      Opts.option[String]("dataset1", help = "Where to find dataset 1")
    val dataset2 =
      Opts.option[String]("dataset2", help = "Where to find dataset 2")
    val nomatch =
      Opts.flag("nomatch", help = "Disable building-matching (useful for saving clipped datasets)").orFalse
    val sharedHistory =
      Opts.flag("shared", help = "Include shared history of the two datasets").orFalse
    val matched1 =
      Opts.flag("matched1", help = "Include parts of dataset 1 that match something in dataset 2").orFalse
    val matched2 =
      Opts.flag("matched2", help = "Include parts of dataset 2 that match something in dataset 1").orFalse
    val only1 =
      Opts.flag("only1", help = "Include parts of dataset 1 that do not match anything in dataset 2").orFalse
    val only2 =
      Opts.flag("only2", help = "Include parts of dataset 2 that do not match anything in dataset 1").orFalse
    val saveDataset1 =
      Opts.option[String]("saveDataset1", help = "Where to save dataset 1").orNone
    val saveDataset2 =
      Opts.option[String]("saveDataset2", help = "Where to save dataset 2").orNone
    val savePolygonTiles =
      Opts.option[String]("savePolygonTiles", help = "Where to save polygon tiles").orNone
    val savePointTiles =
      Opts.option[String]("savePointTiles", help = "Where to save point tiles").orNone

    (clipGeometry, dataset1, dataset2,
      nomatch, sharedHistory, matched1, matched2, only1, only2,
      saveDataset1, saveDataset2,
      savePolygonTiles, savePointTiles)
      .mapN({
        (_clipGeometry, dataset1, dataset2,
          nomatch, sharedHistory, matched1, matched2, only1, only2,
          saveDataset1, saveDataset2,
          savePolygonTiles, savePointTiles) =>

        val clipGeometry: Option[Geometry] = _clipGeometry
          .flatMap({ str => Some(scala.io.Source.fromFile(str).mkString.parseGeoJson[Geometry]) })

        val dataset1Features = { // most-likely OSM
          if (dataset1.endsWith(".orc")) {
            val (ns1, ws1, rs1) = vectorpipe.osm.fromORC(dataset1)(ss).get
            vectorpipe
              .osm.features(ns1.filter(Util.filterfn2(clipGeometry)), ws1, rs1).geometries
              .filter({ feature => feature.data.tags.contains("building") })
              .filter(Util.filterfn1(clipGeometry))
          }
          else ss.sparkContext.objectFile[OSMFeature](dataset1)
        }.filter({ f: OSMFeature =>
          (f.geom, clipGeometry) match {
            case (p: Polygon, Some(g)) => (p.vertices.length > 4) && (p.intersects(g))
            case (p: Polygon, None) => (p.vertices.length > 4)
            case _ => false
          }
        })

        val dataset2Features = {
          if (dataset2.endsWith(".orc")) {
            val (ns1, ws1, rs1) = vectorpipe.osm.fromORC(dataset2)(ss).get
            vectorpipe
              .osm.features(ns1.filter(Util.filterfn2(clipGeometry)), ws1, rs1).geometries
              .filter({ feature => feature.data.tags.contains("building") })
              .filter(Util.filterfn1(clipGeometry))
          }
          else ss.sparkContext.objectFile[OSMFeature](dataset2)
        }.filter({ f: OSMFeature =>
          (f.geom, clipGeometry) match {
            case (p: Polygon, Some(g)) => (p.vertices.length > 4) && (p.intersects(g))
            case (p: Polygon, None) => (p.vertices.length > 4)
            case _ => false
          }
        })

        saveDataset1 match {
          case Some(filename) => dataset1Features.saveAsObjectFile(filename)
          case None =>
        }

        saveDataset2 match {
          case Some(filename) => dataset2Features.saveAsObjectFile(filename)
          case None =>
        }

        if (!nomatch) {

          val features = dataset1Features.map({ f => (f, 0) })
            .union(dataset2Features.map({ f => (f, 1) }))
            .partitionBy(new QuadTreePartitioner(Range(0,24).toSet, 4099)) // 4099 is the smallest prime larger than 2**12 = 4096
            .mapPartitions({ (it: Iterator[(OSMFeature, Int)]) => // XXX misses possible matches in ancestral quad tree boxes

              // Data
              val array = it.toArray

              // Venn diagram of data
              val (left, intersection, right) = {
                val middle = mutable.Set.empty[OSMFeature]
                val middle2 = mutable.Set.empty[OSMFeature]

                var i = 0; while (i < array.length) {
                  val a = array(i)._1
                  var j = i+1; while (j < array.length) {
                    val b = array(j)._1
                    if (a.geom == b.geom) {
                      middle += a
                      middle2 += b
                    }
                    j = j + 1
                  }
                  i = i + 1
                }

                val leftMinusMiddle = array
                  .filter(_._2 == 0)
                  .filter({ case (f, _) => !(middle.contains(f) || middle2.contains(f)) })
                  .map(_._1)
                val rightMinusMiddle = array
                  .filter(_._2 == 1)
                  .filter({ case (f, _) => !(middle.contains(f) || middle2.contains(f)) })
                  .map(_._1)

                (leftMinusMiddle, middle.toArray, rightMinusMiddle)
              }

              // Compute Relative Similarities
              val r = {
                val data = Array.ofDim[Double](left.length, right.length, intersection.length)
                var k = 0; while (k < intersection.length) {
                  val c = intersection(k)
                  val cCentroid = Centroid.getCentroid(c.jtsGeom)
                  var i = 0; while (i < left.length) {
                    val a = left(i)
                    val aCentroid = Centroid.getCentroid(a.jtsGeom)
                    val dist1 = aCentroid.distance(cCentroid)
                    val (vx, vy) = (aCentroid.x - cCentroid.x, aCentroid.y - cCentroid.y)
                    val absv = math.sqrt(vx*vx + vy*vy)
                    var j = 0; while (j < right.length) {
                      val b = right(j)
                      val bCentroid = Centroid.getCentroid(b.jtsGeom)
                      val dist2 = bCentroid.distance(cCentroid)
                      val (ux, uy) = (bCentroid.x - cCentroid.x, bCentroid.y - cCentroid.y)
                      val absu = math.sqrt(ux*ux + uy*uy)
                      val dot = ((vx*ux + vy*uy)/(absv*absu) + 1.0)/2.0
                      val dist = math.min(dist1/dist2,dist2/dist1)
                      data(i)(j)(k) = dot*dist
                      j = j + 1
                    }
                    i = i + 1
                  }
                  k = k + 1
                }
                data
              }

              // Compute Support
              val q = {
                val data = Array.ofDim[Double](left.length, right.length)
                var i = 0; while (i < left.length) {
                  var j = 0; while (j < right.length) {
                    data(i)(j) = r(i)(j).sum
                    j = j + 1
                  }
                  i = i + 1
                }
                val maximum = if (left.nonEmpty && right.nonEmpty) data.map(_.max).max; else 1.0
                data.map({ a => a.map({ x => x/maximum }) })
              }

              // Compute Probabilities
              val p = {
                val data = Array.ofDim[Double](left.length, right.length)
                var i = 0; while (i < left.length) {
                  val a = left(i).geom.asInstanceOf[Polygon]
                  var j = 0; while (j < right.length) {
                    val b = right(j).geom.asInstanceOf[Polygon]
                    val p1 = 1.0 - VertexMatching.score(a, b)
                    val (p2, p3) = VolumeMatching.data(a, b)
                    data(i)(j) = (math.max(p1, math.max(p2, p3)) + q(i)(j))/2.0
                    // data(i)(j) = math.max(p1, math.max(p2, p3)) // XXX
                    j = j + 1
                  }
                  i = i + 1
                }
                data
              }

              val retval = mutable.ArrayBuffer.empty[(OSMFeature, (Double, Long))]
              // val retval = mutable.ArrayBuffer.empty[(OSMFeature, (Double, Geometry))]

              if (sharedHistory) {
                intersection.foreach({ f =>
                  val geom = f.geom
                  val data = f.data
                  val f2 = new OSMFeature(geom, tags.set(data.tags + ("dataset" -> "both"))(data))
                  val pair = (f2, (1.0, 0L))
                  retval.append(pair)
                })
              }

              // left
              if (matched1 || only1) {
                var i = 0; while (i < left.length) {
                  val f1 = left(i)
                  val geom = f1.geom
                  val data = f1.data
                  val f2 = new OSMFeature(geom, tags.set(data.tags + ("dataset" -> "left"))(data))
                  var unmatched = true
                  var bestProb = 0.0
                  var j = 0; while (j < right.length) {
                    val geom2 = right(j).geom
                    if ((p(i)(j) > 0.50) && (geom.distance(geom2) < 0.01)) { // XXX
                      // val pair2 = (p(i)(j), geom2)
                      unmatched = false
                      val pair2 = (p(i)(j), right(j).data.id)
                      val pair = (f2, pair2)
                      if (matched1) retval.append(pair)
                    }
                    else if (geom.distance(geom2) < 0.01) // XXX
                      bestProb = math.max(bestProb, p(i)(j))
                    j = j + 1
                  }
                  if (unmatched && only1) retval.append((f2,(bestProb, -1L)))
                  i = i + 1
                }
              }

              // right
              if (matched2 || only2) {
                var j = 0; while (j < right.length) {
                  val f1 = right(j)
                  val geom = f1.geom
                  val data = f1.data
                  val f2 = new OSMFeature(geom, tags.set(data.tags + ("dataset" -> "right"))(data))
                  var unmatched = true
                  var bestProb = 0.0
                  var i = 0; while (i < left.length) {
                    val geom2 = left(i).geom
                    if ((p(i)(j) > 0.50) && (geom.distance(geom2) < 0.01)) { // XXX
                      // val pair2 = (p(i)(j), geom2)
                      unmatched = false
                      val pair2 = (p(i)(j), left(i).data.id)
                      val pair = (f2, pair2)
                      if (matched2)
                        retval.append(pair)
                    }
                    else if (geom.distance(geom2) < 0.01) // XXX
                      bestProb = math.max(bestProb, p(i)(j))
                    i = i + 1
                  }
                  if (unmatched && only2) retval.append((f2,(bestProb, -1L)))
                  j = j + 1
                }
              }

              retval.toIterator
            }, preservesPartitioning = true)
            .groupByKey // XXX can be optimized away by changing inner loop

          // Building tiles
          savePolygonTiles match {
            case Some(uri) => {

              val tiles: RDD[GenerateVT.VTF[Geometry]] =
                features
                  .map({ case (building, buildingsItr) =>
                    val buildings = buildingsItr.toArray.sortBy({ case (prob, _) => -prob })
                    val bestMatch = buildings.head
                    val dataset = building.data.tags.getOrElse("dataset", throw new Exception)
                    val bestMatchProb = bestMatch._1
                    val bestMatchId = bestMatch._2
                    val displayNumber = dataset match {
                      case "left" => bestMatchProb
                      case "both" => 1.0
                      case "right" => 2.0 - bestMatchProb
                    }

                    Feature(
                      building.geom.reproject(LatLng, WebMercator),
                      Map(
                        "__id" -> VInt64(building.data.id),
                        "dataset" -> VString(dataset),
                        "totalMatches" -> VInt64(buildings.length),
                        "bestMatchProb" -> VDouble(bestMatchProb),
                        "bestMatchId" -> VInt64(bestMatchId),
                        "displayNumber" -> VDouble(displayNumber)
                      )
                    )
                  })

              features.count

              Range(19,14,-1).foreach({ z =>
                println(s"Polygon Level $z")
                val layoutScheme = ZoomedLayoutScheme(WebMercator, 512)
                val maxLayoutLevel = layoutScheme.levelForZoom(z)
                val LayoutLevel(zoom, layout) = maxLayoutLevel
                val keyed = GenerateVT.keyToLayout(tiles, layout)

                GenerateVT.saveHadoop(
                  GenerateVT.makeVectorTiles(keyed, layout, "bm"),
                  zoom,
                  uri
                )
              })
            }
            case _ =>
          }

          // Heatmap tiles
          savePointTiles match {
            case Some(uri) => {
              var points = Downsample.transmute(features.keys)

              Range(19,-1,-1).foreach({ z =>
                points = Downsample(points, z)
                println(s"Heatmap Level $z: ${points.count} points")

                val heatmapTiles: RDD[GenerateVT.VTF[Geometry]] = points.map({ point =>
                  val dataset = point.data.tags.getOrElse("dataset", throw new Exception)
                  val multiplicity = point.data.tags.getOrElse("multiplicity", throw new Exception).toLong
                  Feature(
                    point.geom.reproject(LatLng, WebMercator),
                    Map(
                      "__id" -> VInt64(point.data.id),
                      "dataset" -> VString(dataset),
                      "multiplicity" -> VInt64(multiplicity)
                    )
                  )
                })
                val layoutScheme = ZoomedLayoutScheme(WebMercator, 512)
                val maxLayoutLevel = layoutScheme.levelForZoom(z)
                val LayoutLevel(zoom, layout) = maxLayoutLevel
                val keyed = GenerateVT.keyToLayout(heatmapTiles, layout)

                GenerateVT.saveHadoop(
                  GenerateVT.makeVectorTiles(keyed, layout, "bm-heat"),
                  zoom,
                  uri
                )
              })
            }
            case _ =>
          }

        }
      })
  }
)
