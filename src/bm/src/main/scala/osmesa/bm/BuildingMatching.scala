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

import com.vividsolutions.jts.geom.Envelope


object BuildingMatching {

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

  def calculateEnvelope(pairs: Iterator[(OSMFeature, Null)]): Iterator[Envelope] =
    VectorJoin.calculateEnvelope(pairs.map({ pair => pair._1 }))

  def main(args: Array[String]): Unit = {
    val ss = sparkSession("Building-Matching")

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val layout = ZoomedLayoutScheme.layoutForZoom(15, WebMercator.worldExtent, 512)

    val clipGeometry: Option[Geometry] =
      if (args.length >= 4) Some(scala.io.Source.fromFile(args(3)).mkString.parseGeoJson[Geometry])
      else None

    val osmFeatures =
      if (args(1).endsWith(".orc")) {
        val (ns1, ws1, rs1) = vectorpipe.osm.fromORC(args(1))(ss).get
        vectorpipe
          .osm.features(ns1.filter(filterfn2(clipGeometry)), ws1, rs1).geometries
          .filter({ feature => feature.data.tags.contains("building") })
          .filter(filterfn1(clipGeometry))
      }
      else ss.sparkContext.objectFile[OSMFeature](args(1))

    val nomeFeatures =
      if (args(2).endsWith(".orc")) {
        val (ns2, ws2, rs2) = vectorpipe.osm.fromORC(args(2))(ss).get
        vectorpipe
          .osm.features(ns2.filter(filterfn2(clipGeometry)), ws2, rs2).geometries
          .filter({ feature => feature.data.tags.contains("building") })
          .filter(filterfn1(clipGeometry))
      }
      else ss.sparkContext.objectFile[OSMFeature](args(2))

    if (args(0) == "match") { // No clip geometry supplied

      val nomeNotOsm = nomeFeatures.map({ f => (f.data.uid, f) })
        .subtractByKey(osmFeatures.map({ f => (f.data.uid, f) }))
        .values.map({ f => (f.geom, f) })
        .subtractByKey(osmFeatures.map({ f => (f.geom, f) }))
        .values
        .filter({ f: OSMFeature =>
          f.geom match {
            case p: Polygon => (p.vertices.length > 4)
            case mp: MultiPolygon => (mp.vertices.length > 4)
            case _ => false
          }
        })

      val osmNotNome = osmFeatures.map({ f => (f.data.uid, f) })
        .subtractByKey(nomeFeatures.map({ f => (f.data.uid, f) }))
        .values.map({ f => (f.geom, f) })
        .subtractByKey(nomeFeatures.map({ f => (f.geom, f) }))
        .values
        .filter({ f: OSMFeature =>
          f.geom match {
            case p: Polygon => (p.vertices.length > 4)
            case mp: MultiPolygon => (mp.vertices.length > 4)
            case _ => false
          }
        })

      println(s"NOME NOT OSM: ${nomeNotOsm.count}")
      println(s"OSM NOT NOME: ${osmNotNome.count}")

      val possibleMatches = nomeNotOsm.map({ f => (f, 0) })
        .union(osmNotNome.map({ f => (f, 1) }))
        .partitionBy(new QuadTreePartitioner(Range(0,24).toSet, 1031))
        .mapPartitions({ (it: Iterator[(OSMFeature, Int)]) =>
          val a = it.toArray
          val ab = scala.collection.mutable.ArrayBuffer.empty[(OSMFeature, OSMFeature)]

          var i = 0; while (i < a.length) {
            val left = a(i)
            var j = i+1; while (j < a.length) {
              val right = a(j)
              if (left._2 != right._2 && left._1.geom.intersects(right._1.geom))
                ab.append((left._1, right._1))
              j = j + 1
            }
            i = i + 1
          }

          ab.toIterator
        }, preservesPartitioning = true)

      println(s"POSSIBLE MATCHES: ${possibleMatches.count}")

      val data = possibleMatches.map({ case (left: OSMFeature, right: OSMFeature) =>
        val h1 = VertexProjection.geometryToGeometry(left.geom, right.geom)
        val h2 = VertexProjection.geometryToGeometry(right.geom, left.geom)
        (left.geom.toGeoJson, right.geom.toGeoJson, h1.toArray.toList, h2.toArray.toList)
      })

      data.take(100).foreach({ case (left: String, right: String, h1: List[Double], h2: List[Double]) =>
        println(left)
        println(right)
        println(s"$h1 $h2")
        println
      })

      // println(s"DATA: ${data.take(100).toList}")

      // val homographies =
      //   some.map({ f: OSMFeature => Homography.kappa(f,f) })

      // println(s"RDD: ${homographies.collect().toList}")
    }
    else if (args(0) == "save") { // Clip geometry supplied
      if (args.length >= 5)
        osmFeatures.saveAsObjectFile(args(4))
      if (args.length >= 6)
        nomeFeatures.saveAsObjectFile(args(5))
    }
    else {
      throw new Exception("Unknown activity")
    }
  }

}
