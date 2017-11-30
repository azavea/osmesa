package osmesa.client

import com.google.common.base.Joiner
import com.vividsolutions.jts.geom.Geometry
import org.geotools.data._
import org.geotools.feature.SchemaException
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.cql2.CQLException
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.geomesa.hbase.data._
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
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.s3.{S3AttributeStore, S3LayerWriter}
import geotrellis.spark.tiling._
import geotrellis.vectortile.VectorTile
import geotrellis.geotools.SimpleFeatureToFeature
import org.geotools.data.simple.SimpleFeatureCollection
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import vectorpipe._
import vectorpipe.util.LayerMetadata
import cats.implicits._
import cats.data.{NonEmptyList => NEL, _}
import Validated._
import com.monovore.decline._
import org.geotools.data.DataStore
import org.geotools.filter.text.ecql.ECQL
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin}
import org.apache.hadoop.hbase.HBaseConfiguration

import java.io.IOException
import java.io.Serializable
import java.util.HashMap
import java.util.Random
import java.net.URI
import java.io.File
import scala.collection.JavaConverters._


object QueryECQL extends CommandApp(
  name = "Run Arbitrary ECQL queries against an OSMesa table",
  header = "Run Arbitrary ECQL queries against an OSMesa table",
  main = {
    val datastoreConfO = Opts.option[URI]("conf", help = "Geotools Datastore Configuration (YAML) URI")
    val typeNameO = Opts.option[String]("type", help = "Geotools SimpleFeatureType Name")
    val queryO = Opts.option[String]("ecql", help = "Geotools Extended Common Query Language string for filtering an OSMesa table")
    val localF = Opts.flag("local", help = "Is this to be run locally?").orFalse

    (datastoreConfO, typeNameO, queryO, localF).mapN({ (dsConfUri, typeName, ecql, local) =>
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
        Util.loadYamlAsDatastoreConf(dsConfUri).andThen({ dsConf =>
          try {
            val dataStore = DataStoreFinder.getDataStore(dsConf).asInstanceOf[HBaseDataStore]
            val featureSource = dataStore.getFeatureSource(typeName)
            val filter = ECQL.toFilter(ecql)
            val query = new org.geotools.data.Query(typeName, filter)

            Valid(featureSource.getFeatures(query))
          } catch {
            case e: org.geotools.filter.text.cql2.CQLException =>
              Invalid(NEL.of(s"Unable to parse provided string as an ECQL filter (${ecql})"))
          }
        }) match {
          case Valid(recs) =>
            println(s"Retrieved records:")
            val iter = recs.features()
            try {
              var count = 0
              while (iter.hasNext()) {
                count += 1
                println(iter.next())
              }
              println("the count", count)
            } finally {
              iter.close()
            }
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
