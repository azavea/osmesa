package osmesa

import com.google.common.base.Joiner
import com.vividsolutions.jts.geom.Geometry
import org.geotools.data._
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.FeatureCollection
import org.geotools.feature.FeatureIterator
import org.geotools.feature.SchemaException
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.cql2.CQLException
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import geotrellis.vector._
import geotrellis.vector.io._
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import vectorpipe._
import vectorpipe.osm.OSMFeature
import spray.json._
import com.monovore.decline._

import org.geotools.data.DataStore


import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.locationtech.geomesa.hbase.data._

import java.io.IOException
import java.io.Serializable
import java.util.HashMap
import java.util.Random
import scala.collection.JavaConverters._

/** A test of the dev setup - ingest roads from a small country's ORC.
  */
object OrcLatestIngestTest {
  def getHBaseDataStoreConf(): Map[String, Serializable] =
    Map(
      "hbase.catalog" -> "suriname-roads"
    )

  def createSimpleFeatureType(simpleFeatureTypeName: String): SimpleFeatureType = {

    // list the attributes that constitute the feature type
    val attributes = List(
      "userId:String",
      "createdate:Date",
      "*geom:LineString:srid=4326",
      "tags:String"
    )

    // create the bare simple-feature type
    val simpleFeatureTypeSchema = Joiner.on(",").join(attributes.asJava)
    val simpleFeatureType =
      DataUtilities.createType(simpleFeatureTypeName, simpleFeatureTypeSchema)

    simpleFeatureType
  }

  val ROAD_TAGS =
    Set(
      "motorway",
      "trunk",
      "motorway_link",
      "trunk_link",
      "primary",
      "secondary",
      "tertiary",
      "primary_link",
      "secondary_link",
      "tertiary_link",
      "service",
      "residential",
      "unclassified",
      "living_street",
      "road"
    )

  def isRoad(feature: OSMFeature): Boolean =
    feature.geom match {
      case _: Line =>
        feature.data.tags.get("highway") match {
          case Some(t) if ROAD_TAGS.contains(t) => true
          case _ => false
        }
      case _ => false
    }

  def main(args: Array[String]): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val orc = "/opt/src/test-data/suriname.orc"

    /* Settings compatible for both local and EMR execution */
    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setAppName("osmesa-ingest-test")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)

    implicit val ss: SparkSession =
      SparkSession.builder
        .config(conf)
        .enableHiveSupport
        .getOrCreate

    try {
      val dsConf = getHBaseDataStoreConf()
      val dataStore = DataStoreFinder.getDataStore(dsConf.asJava)

      val simpleFeatureTypeName = "OsmRoadLines"
      val simpleFeatureType = createSimpleFeatureType(simpleFeatureTypeName)
      dataStore.createSchema(simpleFeatureType)

      val df = ss.read.orc(orc)

      val (ns,ws,rs) = osm.fromDataFrame(df)

      /* Assumes that OSM ORC is in LatLng */
      val feats: RDD[osm.OSMFeature] =
        osm.features(
          ns.repartition(100),
          ws.repartition(10),
          rs
        ).geometries

      feats.foreachPartition { part =>
        val mDsConf = getHBaseDataStoreConf()
        val mDataStore = DataStoreFinder.getDataStore(mDsConf.asJava)
        val featureStore = mDataStore.getFeatureSource(simpleFeatureTypeName).asInstanceOf[FeatureStore[SimpleFeatureType,SimpleFeature]]
        val featureCollection = new DefaultFeatureCollection()

        part.filter(isRoad).foreach { feature =>
          val sft = createSimpleFeatureType(simpleFeatureTypeName)
          val simpleFeature = SimpleFeatureBuilder.build(sft,  Array[Object](), feature.data.id.toString)
          simpleFeature.getUserData().put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)

          simpleFeature.setAttribute("uid", feature.data.uid)
          simpleFeature.setAttribute("createdate", new DateTime(feature.data.timestamp.toEpochMilli))
          simpleFeature.setAttribute("tags", feature.data.tags.map { case (k, v) => s"$k=$v" }.mkString(";"))
          simpleFeature.setAttribute("geom", feature.geom.jtsGeom)

          featureCollection.add(simpleFeature)
        }
        featureStore.addFeatures(featureCollection)
      }
    } finally {
      ss.stop()
    }

    println("Done.")
  }
}
