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

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.locationtech.geomesa.hbase.data._

import java.io.IOException
import java.io.Serializable
import java.util.HashMap
import java.util.Random
import scala.collection.JavaConverters._

/** Recreation of the tutorial here: https://github.com/geomesa/geomesa-tutorials/blob/d7dd65d215d05f2cdaa41aff176b6c82f722bb31/geomesa-quickstart-hbase/src/main/java/com/example/geomesa/hbase/HBaseQuickStart.java
  * to verify the development environment is working.
  */
object GeomesaHBaseTutorial {
  val TABLE_NAME = "bigtable.table.name".replace(".", "_")

  // sub-set of parameters that are used to create the HBase DataStore
  val HBASE_CONNECTION_PARAMS = Array(TABLE_NAME)

  def getHBaseDataStoreConf(): Map[String, Serializable] = {
    (HBASE_CONNECTION_PARAMS.map { param =>
      (param.replace("_", "."), "geomesatest")
    }).toMap
  }

  def createSimpleFeatureType(simpleFeatureTypeName: String): SimpleFeatureType = {

    // list the attributes that constitute the feature type
    val attributes = List(
      "Who:String",
      "What:java.lang.Long",     // some types require full qualification (see DataUtilities docs)
      "When:Date",               // a date-time field is optional, but can be indexed
      "*Where:Point:srid=4326",  // the "*" denotes the default geometry (used for indexing)
      "Why:String"               // you may have as many other attributes as you like...
    )

    // create the bare simple-feature type
    val simpleFeatureTypeSchema = Joiner.on(",").join(attributes.asJava)
    val simpleFeatureType =
      DataUtilities.createType(simpleFeatureTypeName, simpleFeatureTypeSchema)

    simpleFeatureType
  }

  def createNewFeatures(simpleFeatureType: SimpleFeatureType, numNewFeatures: Int): FeatureCollection[SimpleFeatureType,SimpleFeature] = {
    val featureCollection = new DefaultFeatureCollection()

    val NO_VALUES = Array[Object]()
    val PEOPLE_NAMES = Array("Addams", "Bierce", "Clemens")
    val SECONDS_PER_YEAR = 365L * 24L * 60L * 60L
    val random = new Random(5771)
    val MIN_DATE = new DateTime(2014, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"))
    val MIN_X = -79.5
    val MIN_Y =  37.0
    val DX = 2.0
    val DY = 2.0

    for(i <- 0 until numNewFeatures) {
      // create the new (unique) identifier and empty feature shell
      val id = "Observation." + Integer.toString(i)
      val simpleFeature = SimpleFeatureBuilder.build(simpleFeatureType, NO_VALUES, id)

      // be sure to tell GeoTools explicitly that you want to use the ID you provided
      simpleFeature.getUserData().put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)

      // populate the new feature's attributes

      // Who: string value
      simpleFeature.setAttribute("Who", PEOPLE_NAMES(i % PEOPLE_NAMES.length))

      // What: long value
      simpleFeature.setAttribute("What", i)

      // Where: location: construct a random point within a 2-degree-per-side square
      val x = MIN_X + random.nextDouble() * DX
      val y = MIN_Y + random.nextDouble() * DY
      val geometry = WKTUtils.read("POINT(" + x + " " + y + ")")
      simpleFeature.setAttribute("Where", geometry)

      // When: date-time:  construct a random instant within a year
      val dateTime = MIN_DATE.plusSeconds((math.round(random.nextDouble() * SECONDS_PER_YEAR)).toInt)
      simpleFeature.setAttribute("When", dateTime.toDate())

      // Why: another string value
      // left empty, showing that not all attributes need values

      // accumulate this new feature in the collection
      featureCollection.add(simpleFeature)
    }

    featureCollection
  }

  def insertFeatures(simpleFeatureTypeName: String,
    dataStore: DataStore,
    featureCollection: FeatureCollection[SimpleFeatureType,SimpleFeature]): Unit = {
    val featureStore = dataStore.getFeatureSource(simpleFeatureTypeName).asInstanceOf[FeatureStore[SimpleFeatureType,SimpleFeature]]
    featureStore.addFeatures(featureCollection)
  }

  def  createFilter(geomField: String, x0: Double, y0: Double, x1: Double, y1: Double,
    dateField: String, t0: String, t1: String,
    attributesQuery: String): Filter = {

    // there are many different geometric predicates that might be used
    // here, we just use a bounding-box (BBOX) predicate as an example.
    // this is useful for a rectangular query area
    val cqlGeometry = s"BBOX($geomField,$x0,$y0,$x1,$y1)"

    // there are also quite a few temporal predicates here, we use a
    // "DURING" predicate, because we have a fixed range of times that
    // we want to query
    val cqlDates = s"($dateField DURING $t0/$t1)"

    // there are quite a few predicates that can operate on other attribute
    // types the GeoTools Filter constant "INCLUDE" is a default that means
    // to accept everything
    val cqlAttributes = if(attributesQuery == null) "INCLUDE" else attributesQuery

    val cql = s"$cqlGeometry AND $cqlDates AND $cqlAttributes"
    CQL.toFilter(cql)
  }

  def queryFeatures(simpleFeatureTypeName: String,
    dataStore: DataStore,
    geomField: String, x0: Double, y0: Double, x1: Double, y1: Double,
    dateField: String, t0: String, t1: String,
    attributesQuery: String): Unit = {
    // construct a (E)CQL filter from the search parameters,
    // and use that as the basis for the query
    val cqlFilter = createFilter(geomField, x0, y0, x1, y1, dateField, t0, t1, attributesQuery)
    val query = new Query(simpleFeatureTypeName, cqlFilter)

    // submit the query, and get back an iterator over matching features
    val featureSource = dataStore.getFeatureSource(simpleFeatureTypeName)
    val featureItr = featureSource.getFeatures(query).features()

    // loop through all results
    var n = 0
    while (featureItr.hasNext()) {
      val feature = featureItr.next()
      println(s"${n}." +
        feature.getProperty("Who").getValue() + "|" +
        feature.getProperty("What").getValue() + "|" +
        feature.getProperty("When").getValue() + "|" +
        feature.getProperty("Where").getValue() + "|" +
        feature.getProperty("Why").getValue())
    }
    featureItr.close()
  }

  def main(args: Array[String]): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    for(ds <- DataStoreFinder.getAllDataStores.asScala) {
      println(ds)
    }

    val conf = HBaseConfiguration.create()
    println(s"""HBASE ZOOKEEPER: ${conf.get("hbase.zookeeper.quorum")}""")
    HBaseAdmin.checkHBaseAvailable(conf)

    // verify that we can see this HBase destination in a GeoTools manner
    val dsConf = getHBaseDataStoreConf()
    val dataStore = DataStoreFinder.getDataStore(dsConf.asJava)
    assert(dataStore != null)

    val hds = dataStore.asInstanceOf[HBaseDataStore]
    println(hds)

    // establish specifics concerning the SimpleFeatureType to store
    val simpleFeatureTypeName = "QuickStart"
    val simpleFeatureType = createSimpleFeatureType(simpleFeatureTypeName)

    hds.manager.setIndices(simpleFeatureType)

    // write Feature-specific metadata to the destination table in HBase
    // (first creating the table if it does not already exist) you only need
    // to create the FeatureType schema the *first* time you write any Features
    // of this type to the table
    println(s"Creating feature-type (schema):  $simpleFeatureTypeName")
    dataStore.createSchema(simpleFeatureType)

    for((key, value) <- hds.manager.lookup) {
      println(s"MANAGER: $key -> $value")
    }

    for(idx <- hds.manager.CurrentIndices) {
      println(s"MANAGER CI: ${idx.supports(simpleFeatureType)} ${idx.name}:${idx.version}")
    }


    for(p <- simpleFeatureType.getUserDataPrefixes) {
      println(s"SFT Prefix: $p")
    }

    for((key, value) <- simpleFeatureType.getUserData.asScala) {
      println(s"SFT UserData: $key -> $value")
    }

    // for(row <- hds.metadata.asInstanceOf[HBaseBackedMetadata[String]].scanRows(None)) {
    //   println(s"MD SCANROW: $row")
    // }

    println(hds.metadata.read(simpleFeatureTypeName, "table.z2.v2"))

    for(ft <- hds.metadata.getFeatureTypes) { println(s"DS FEATURE TYPE: $ft") }

    // create new features locally, and add them to this table
    println("Creating new features")
    val featureCollection = createNewFeatures(simpleFeatureType, 1000)
    println("Inserting new features")
    insertFeatures(simpleFeatureTypeName, dataStore, featureCollection)

    // query a few Features from this table
    println("Submitting query")
    queryFeatures(simpleFeatureTypeName, dataStore,
      "Where", -78.5, 37.5, -78.0, 38.0,
      "When", "2014-07-01T00:00:00.000Z", "2014-09-30T23:59:59.999Z",
      "(Who = 'Bierce')")
  }
}
