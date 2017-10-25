package osmesa.ingest

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
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
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
import org.locationtech.geomesa.hbase.data._

import java.io.IOException
import java.io.Serializable
import java.util.HashMap
import java.util.Random
import java.net.URI
import java.io.File
import scala.collection.JavaConverters._


object RemoveSchema extends CommandApp(
  name = "Remove an OSMesa schema and associated records",
  header = "Remove an OSMesa schema and associated records",
  main = {
    val datastoreConfO = Opts.option[URI]("conf", help = "Geotools Datastore Configuration (YAML) URI")
    val typeNameO = Opts.option[String]("type", help = "Geotools SimpleFeatureType Name")
    val localF = Opts.flag("local", help = "Is this to be run locally?").orFalse

    (datastoreConfO, typeNameO, localF).mapN({ (dsConfUri, typeName, local) =>
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
        Util.loadYamlAsDatastoreConf(dsConfUri).map({ dsConf =>
          val datastore = DataStoreFinder.getDataStore(dsConf)
          datastore.removeSchema(typeName)
        }) match {
          case Valid(_) =>
            println(s"Successfully dropped the schema named $typeName")
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
