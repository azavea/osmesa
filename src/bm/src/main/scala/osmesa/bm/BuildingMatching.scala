package osmesa.bm

import geotrellis.proj4.WebMercator
import geotrellis.spark._
import geotrellis.spark.join._
import geotrellis.spark.tiling._
import geotrellis.vector._

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import vectorpipe._
import vectorpipe.osm._
import monocle.macros.GenLens // XXX

import com.vividsolutions.jts.geom.Envelope

import org.apache.spark._
import org.apache.spark.rdd._


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

  def match1(a: OSMFeature, b: OSMFeature): Boolean =
    (a.data.uid == b.data.uid) || (a.geom == b.geom)

  def metapred(a: Envelope, b: Envelope): Boolean = a.intersects(b)

  implicit def conversion(a: OSMFeature): Geometry = a.geom

  def main(args: Array[String]): Unit = {
    val ss = sparkSession("Building Matching")
    val layout = ZoomedLayoutScheme.layoutForZoom(15, WebMercator.worldExtent, 512)
    val (ns1, ws1, rs1) = vectorpipe.osm.fromORC(args(0))(ss).get // old
    val (ns2, ws2, rs2) = vectorpipe.osm.fromORC(args(1))(ss).get // new
    val oldFeatures = vectorpipe
      .osm.features(ns1, ws1, rs1).geometries
      .filter({ feature => feature.data.tags.contains("building") })
      .repartition(1<<6)
    val newFeatures = vectorpipe
      .osm.features(ns2, ws2, rs2).geometries
      .filter({ feature => feature.data.tags.contains("building") })
      .repartition(1<<6)

    val possibleAdditions = newFeatures.map({ f => (f.data.uid, f) })
      .subtractByKey(oldFeatures.map({ f => (f.data.uid, f) }))
      .values.map({ f => (f.geom, f) })
      .subtractByKey(oldFeatures.map({ f => (f.geom, f) }))
      .values

    val some = possibleAdditions
      .filter({ f: OSMFeature =>
        f.geom match {
          case p: Polygon => (p.vertices.length > 4)
          case mp: MultiPolygon => (mp.vertices.length > 4)
          case _ => false
        }
      })
    val head = some.first

    // println(s"POSSIBLE ADDITIONS: ${possibleAdditions.count}")

    val homographies =
      some.map({ f: OSMFeature => Homography.kappa(f,f) })

    println(s"RDD: ${homographies.collect().toList}")
  }

}
