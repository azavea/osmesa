package osmesa.bm

import geotrellis.proj4.WebMercator
import geotrellis.spark._
import geotrellis.spark.join._
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.vector.io._

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


object Util {

  val MAGIC = 1e-4

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
    val save =
      Opts.flag("save", help = "Save (but do not match) clipped datasets").orFalse
    val saveDataset1 =
      Opts.option[String]("saveDataset1", help = "Where to save clipped dataset 1").orNone
    val saveDataset2 =
      Opts.option[String]("saveDataset2", help = "Where to save clipped dataset 2").orNone

    (clipGeometry, dataset1, dataset2, save, saveDataset1, saveDataset2).mapN({ (_clipGeometry, dataset1, dataset2, save, saveDataset1, saveDataset2) =>

      val clipGeometry: Option[Geometry] = _clipGeometry
        .flatMap({ str => Some(scala.io.Source.fromFile(str).mkString.parseGeoJson[Geometry]) })

      val dataset1Features =
        if (dataset1.endsWith(".orc")) {
          val (ns1, ws1, rs1) = vectorpipe.osm.fromORC(dataset1)(ss).get
          vectorpipe
            .osm.features(ns1.filter(Util.filterfn2(clipGeometry)), ws1, rs1).geometries
            .filter({ feature => feature.data.tags.contains("building") })
            .filter(Util.filterfn1(clipGeometry))
        }
        else ss.sparkContext.objectFile[OSMFeature](dataset1)

      val dataset2Features =
        if (dataset2.endsWith(".orc")) {
          val (ns1, ws1, rs1) = vectorpipe.osm.fromORC(dataset2)(ss).get
          vectorpipe
            .osm.features(ns1.filter(Util.filterfn2(clipGeometry)), ws1, rs1).geometries
            .filter({ feature => feature.data.tags.contains("building") })
            .filter(Util.filterfn1(clipGeometry))
        }
        else ss.sparkContext.objectFile[OSMFeature](dataset2)

      if (!save) {

        val data = dataset1Features.map({ f => (f, 0) })
          .union(dataset2Features.map({ f => (f, 1) }))
          .partitionBy(new QuadTreePartitioner(Range(0,24).toSet, 4099))
          .mapPartitions({ (it: Iterator[(OSMFeature, Int)]) =>
            val a = it.toArray
            val ab = scala.collection.mutable.ArrayBuffer.empty[(Polygon, (String, Double, Polygon))]

            var i = 0; while (i < a.length) {
              val left = a(i)
              val leftFeature = left._1
              val leftGeom = leftFeature.geom.asInstanceOf[Polygon]
              var j = i+1; while (j < a.length) {
                val right = a(j)
                val rightFeature = right._1
                val rightGeom = rightFeature.geom.asInstanceOf[Polygon]
                if (left._2 != right._2) { // Ensure that pairs are from different datasets
                  val (a1, a2) = VolumeMatching.data(leftGeom, rightGeom)
                  lazy val dist = leftGeom.distance(rightGeom) / Util.MAGIC
                  lazy val vm = VertexMatching.score(leftGeom, rightGeom) + dist*dist*dist
                  lazy val vp = VertexProjection.score(leftGeom, rightGeom) + dist*dist*dist

                  if (math.min(a1, a2) > 0.90) // Volume match
                    ab.append((leftGeom, ("0 volume match", math.min(a1,a2), rightGeom)))
                  else if (a1 > 0.90) // superset
                    ab.append((leftGeom, ("1 superset", a1, rightGeom)))
                  else if (a2 > 0.90) // subset
                    ab.append((leftGeom, ("2 subset", a2, rightGeom)))
                  else if (vm < 0.15) // strong volume match
                    ab.append((leftGeom, ("3 strong vertex match", vm, rightGeom)))
                  else if (vm < 0.60) // weak volume match
                    ab.append((leftGeom, ("4 weak vertex match", vm, rightGeom)))
                  else if (vp < 0.15) // strong projection match
                    ab.append((leftGeom, ("5 strong projection match", vp, rightGeom)))
                  else if (vp < 0.60) // weak volume match
                    ab.append((leftGeom, ("6 weak projection match", vp, rightGeom)))
                }
                j = j + 1
              }
              i = i + 1
            }
            ab.toIterator
          }, preservesPartitioning = true)
          .groupByKey // XXX can be optimized away by changing inner loop
          .map({ case (b, itr) => (b, itr.toArray.sortBy({ _._1 })) })

      } else if (save) {
        saveDataset1 match {
          case Some(filename) => dataset1Features.saveAsObjectFile(filename)
          case None =>
        }
        saveDataset2 match {
          case Some(filename) => dataset2Features.saveAsObjectFile(filename)
          case None =>
        }
      }

    })

    // if (args(0) == "match") { // MATCH

    //   val nomeNotOsm = nomeFeatures.map({ f => (f.geom, f) })
    //     .subtractByKey(osmFeatures.map({ f => (f.geom, f) }))
    //     .values
    //     .filter({ f: OSMFeature =>
    //       f.geom match {
    //         case p: Polygon => (p.vertices.length > 4)
    //         case _ => false
    //       }
    //     })

    //   val osmNotNome = osmFeatures.map({ f => (f.geom, f) })
    //     .subtractByKey(nomeFeatures.map({ f => (f.geom, f) }))
    //     .values
    //     .filter({ f: OSMFeature =>
    //       f.geom match {
    //         case p: Polygon => (p.vertices.length > 4)
    //         case _ => false
    //       }
    //     })

    //   val data = nomeNotOsm.map({ f => (f, 0) })
    //     .union(osmNotNome.map({ f => (f, 1) }))
    //     .partitionBy(new QuadTreePartitioner(Range(0,24).toSet, 4099))
    //     .mapPartitions({ (it: Iterator[(OSMFeature, Int)]) =>
    //       val a = it.toArray
    //       var ab = scala.collection.mutable.ArrayBuffer.empty[(Polygon, (String, Double, Polygon))]

    //       var i = 0; while (i < a.length) {
    //         val left = a(i)
    //         val leftFeature = left._1
    //         val leftGeom = leftFeature.geom.asInstanceOf[Polygon]
    //         var eligible = true
    //         var j = i+1; while (j < a.length) {
    //           val right = a(j)
    //           val rightFeature = right._1
    //           val rightGeom = rightFeature.geom.asInstanceOf[Polygon]
    //           if (left._2 != right._2) { // Ensure that pairs are from different datasets
    //             if (leftGeom == rightGeom) eligible = false // Not interested in trivial matches (shared history)
    //             else {
    //               val (a1, a2) = VolumeMatching.data(leftGeom, rightGeom)
    //               lazy val dist = leftGeom.distance(rightGeom) / MAGIC
    //               lazy val vm = VertexMatching.score(leftGeom, rightGeom) + dist*dist*dist
    //               lazy val vp = VertexProjection.score(leftGeom, rightGeom) + dist*dist*dist

    //               if (math.min(a1, a2) > 0.90) // Volume match
    //                 ab.append((leftGeom, ("0 volume match", math.min(a1,a2), rightGeom)))
    //               else if (a1 > 0.90) // superset
    //                 ab.append((leftGeom, ("1 superset", a1, rightGeom)))
    //               else if (a2 > 0.90) // subset
    //                 ab.append((leftGeom, ("2 subset", a2, rightGeom)))
    //               else if (vm < 0.15) // strong volume match
    //                 ab.append((leftGeom, ("3 strong vertex match", vm, rightGeom)))
    //               else if (vm < 0.60) // weak volume match
    //                 ab.append((leftGeom, ("4 weak vertex match", vm, rightGeom)))
    //               else if (vp < 0.15) // strong projection match
    //                 ab.append((leftGeom, ("5 strong projection match", vp, rightGeom)))
    //               else if (vp < 0.60) // weak volume match
    //                 ab.append((leftGeom, ("6 weak projection match", vp, rightGeom)))
    //             }
    //           }
    //           j = j + 1
    //         }
    //         if (!eligible) ab = ab.filter(_._1 != leftGeom) // XXX
    //         i = i + 1
    //       }
    //       ab.toIterator
    //     }, preservesPartitioning = true)
    //     .groupByKey // XXX can be optimized away by changing inner loop
    //     .map({ case (b, itr) => (b, itr.toArray.sortBy({ _._1 })) })

    //   data.collect.foreach({ case (b: Polygon, bm: Array[(String, Double, Polygon)]) =>
    //     println(s"${b.toGeoJson.hashCode} ${b.toGeoJson}")
    //     bm.sortBy(_._1)
    //       .foreach({ case (str, x, b2) =>
    //         println(s"\t ${b2.toGeoJson.hashCode} ${b2.toGeoJson} $str $x")
    //       })
    //     println
    //   })
    // }
    // else if (args(0) == "save") { // SAVE
    //   if (args.length >= 5)
    //     osmFeatures.saveAsObjectFile(args(4))
    //   if (args.length >= 6)
    //     nomeFeatures.saveAsObjectFile(args(5))
    // }
    // else throw new Exception("Unknown activity")

  }
)
