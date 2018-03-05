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

import osmesa.GenerateVT


object Util {

  val k = 32

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

  // https://en.wikipedia.org/wiki/Error_function#Approximation_with_elementary_functions
  def erf(x: Double): Double = {
    val a1 = 0.278393
    val a2 = 0.230389
    val a3 = 0.000972
    val a4 = 0.078108
    val denom = (1 + a1*x + a2*x*x + a3*x*x*x + a4*x*x*x*x)
    1.0 / (denom*denom*denom*denom)
  }

}

object BuildingMatching extends CommandApp(
  name = "Building-Matching",
  header = "Match Buildings",
  main = {
    val ss = Util.sparkSession("Building-Matching")

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
    val saveDataset1 =
      Opts.option[String]("saveDataset1", help = "Where to save clipped dataset 1").orNone
    val saveDataset2 =
      Opts.option[String]("saveDataset2", help = "Where to save clipped dataset 2").orNone

    (clipGeometry, dataset1, dataset2, nomatch, saveDataset1, saveDataset2)
      .mapN({ (_clipGeometry, dataset1, dataset2, nomatch, saveDataset1, saveDataset2) =>

        val clipGeometry: Option[Geometry] = _clipGeometry
          .flatMap({ str => Some(scala.io.Source.fromFile(str).mkString.parseGeoJson[Geometry]) })

        val dataset1Features = {
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

          val features: RDD[GenerateVT.VTF[Geometry]] = dataset1Features.map({ f => (f, 0) })
            .union(dataset2Features.map({ f => (f, 1) }))
            .partitionBy(new QuadTreePartitioner(Range(0,24).toSet, 4099))
            .mapPartitions({ (it: Iterator[(OSMFeature, Int)]) =>
              val a = it.toArray
              val I = a.filter({ _._2 == 0 }).map({ _._1 })
              val J = a.filter({ _._2 == 1 }).map({ _._1 })
              val p = Array.ofDim[Double](I.length,J.length,2)
              val q = Array.ofDim[Double](I.length,J.length,2)
              val r = Array.ofDim[Double](I.length,J.length,Util.k,Util.k)
              val Ni = I.map({ f1 =>
                I.zipWithIndex
                  .map({ case (f2, h) => (f1.geom.distance(f2.geom), h) })
                  .sortBy(_._1)
                  .take(Util.k)
              }) // XXX quadratic, but okay for now
              val Nj = J.map({ f1 =>
                J.zipWithIndex
                  .map({ case (f2, k) => (f1.geom.distance(f2.geom), k) })
                  .sortBy(_._1)
                  .take(Util.k)
              })

              var i = 0; while (i < I.length) {
                val leftFeature = I(i)
                val leftGeom = leftFeature.geom.asInstanceOf[Polygon]

                var j = 0; while (j < J.length) {
                  val rightFeature = J(j)
                  val rightGeom = rightFeature.geom.asInstanceOf[Polygon]

                  // Initial Probabilities
                  val (a1, a2) = VolumeMatching.data(leftGeom, rightGeom)
                  val vm = 1.0 - VertexMatching.score(leftGeom, rightGeom)
                  p(i)(j)(0) = math.max(a1, math.max(a2, vm)) // initial probabilities

                  // Local Structure (Relative Similarity)
                  val diameter: Double = {
                    val diameter1: Double = Ni(i).map(_._1).max
                    val diameter2: Double = Nj(j).map(_._1).max
                    math.max(diameter1, diameter2)
                  }
                  var h = 0; while (h < Util.k) {
                    var k = 0; while (k < Util.k) {
                      r(i)(j)(h)(k) = (1.0 - math.abs(Ni(i)(h)._2 - Nj(j)(k)._2)) / diameter
                      k = k + 1
                    }
                    h = h + 1
                  }
                  j = j + 1
                }
                i = i + 1
              }

              i = 0; while (i < I.length) {
                var j = 0; while (j < J.length) {

                  // Support
                  q(i)(j)(0) = {
                    Ni(i).flatMap({ case (_, h) =>
                      Nj(j).map({ case (_, k) =>
                        r(i)(j)(h)(k) * p(i)(j)(0)
                      })
                    }).sum
                  }

                  j = j + 1
                }
                i = i + 1
              }

              Array.empty[(OSMFeature, (String, Double, OSMFeature))].toIterator // XXX
            }) // XXX , preservesPartitioning = true)
            .groupByKey // XXX can be optimized away by changing inner loop
            .map({ case (b, itr) =>
              // val best = itr.toArray.sortBy({ _._1 }).head
              Feature(
                b.geom.reproject(LatLng, WebMercator),
                Map(
                  "__id" -> VInt64(b.data.uid),
                  "matches" -> VInt64(itr.toArray.length)
                )
              )
            })

          val layoutScheme = ZoomedLayoutScheme(WebMercator, 512)
          val maxLayoutLevel = layoutScheme.levelForZoom(19)
          val LayoutLevel(zoom, layout) = maxLayoutLevel
          val keyed = GenerateVT.keyToLayout(features, layout)

          GenerateVT.saveHadoop(
            GenerateVT.makeVectorTiles(keyed, layout, "name"),
            zoom,
            "/tmp/tiles"
          )

        }
      })
  }
)
