package osmesa.client

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
import vectorpipe.util.LayerMetadata
import spray.json._
import com.monovore.decline._
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.s3.{S3AttributeStore, S3LayerWriter}
import geotrellis.spark.tiling._
import geotrellis.vectortile.VectorTile
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import cats.implicits._
import cats.data._
import Validated._
import com.monovore.decline._
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.locationtech.geomesa.hbase.data._

import java.io.IOException
import java.io.Serializable
import java.util.HashMap
import java.util.Random
import java.net.URI
import java.io.File
import scala.collection.JavaConverters._


object WayDiff extends CommandApp(
  name = "OSMesa Way (diff) Ingest",
  header = "Ingest OSM ORC files into GeoMesa/HBase",
  main = {
    val orcO = Opts.option[URI]("orc", help = "ORC file URI")
    val datastoreConfO = Opts.option[URI]("conf", help = "Geotools Datastore Configuration (YAML) URI")
    val localF = Opts.flag("local", help = "Is this to be run locally?").orFalse

    (orcO, datastoreConfO, localF).mapN({ (orcUri, dsConfUri, local) =>
      /* Settings compatible for both local and EMR execution */
      val sc = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("osmesa-ingest-test")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)

      implicit val ss: SparkSession =
        SparkSession.builder
          .config(sc)
          .enableHiveSupport
          .getOrCreate

      /* Necessary for locally reading ORC files off S3 */
      if (local) useS3(ss)

      try {
        (Util.loadYamlAsDatastoreConf(dsConfUri), Util.loadOrcAsDataFrame(ss, orcUri)).mapN({ (dsConf, df) =>
          println("dsconf", dsConf)
          val dataStore = DataStoreFinder.getDataStore(dsConf)

          val simpleFeatureTypeName = "OsmRoadLines"
          val simpleFeatureType = OsmFeatureTypes.osmLineStringFeatureType(simpleFeatureTypeName)

          dataStore.createSchema(simpleFeatureType)

          val (ns,ws,rs) = osm.fromDataFrame(df)

          /* Assumes that OSM ORC is in LatLng */
          val feats: RDD[osm.OSMFeature] =
            osm.toFeatures(
              VectorPipe.logToLog4j,
              ns.map(_._2).repartition(100),
              ws.map(_._2).repartition(10),
              rs.map(_._2)
            )

          feats.foreachPartition { part =>
            // Repeat per-partition due to serialization difficulties
            val mDataStore = DataStoreFinder.getDataStore(dsConf)
            val featureStore = mDataStore.getFeatureSource(simpleFeatureTypeName).asInstanceOf[FeatureStore[SimpleFeatureType,SimpleFeature]]
            val featureCollection = new DefaultFeatureCollection()

            part.filter(TagFilters.isRoad).foreach { feature =>
              val sft = OsmFeatureTypes.osmLineStringFeatureType(simpleFeatureTypeName)
              val featureId = s"${feature.data.meta.id}-${feature.data.meta.userId}-${feature.data.meta.version}"
              val sf = {
                val sfb = new SimpleFeatureBuilder(sft)
                sfb.add(feature.data.meta.id)
                sfb.add(feature.data.meta.user)
                sfb.add(feature.data.meta.userId)
                sfb.add(feature.data.meta.changeSet)
                sfb.add(feature.data.meta.version)
                sfb.add(new java.util.Date(feature.data.meta.timestamp.toEpochMilli))
                sfb.add(feature.geom.jtsGeom)
                sfb.add(feature.data.tagMap)
                sfb.buildFeature(featureId)
              }
              featureCollection.add(sf)
            }
            featureStore.addFeatures(featureCollection)
          }
          ()
        }) match {
          case Valid(_) => println("Data ingest complete")
          case Invalid(nel) => nel.map({ error =>
            println(error)
          })
        }
      } finally {
        ss.stop()
      }
    })
  }
)
