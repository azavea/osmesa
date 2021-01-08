package osmesa.analytics

import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.proj4.util.UTM
import geotrellis.vector._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._
import org.locationtech.geomesa.spark.jts.{udf => _, _}
import org.locationtech.jts.geom.Geometry
import osmesa.analytics.stats.functions._
import vectorpipe.functions.osm._

package object stats {

  val transformLatLngToUtm = udf { g: Geometry =>
    val centroid = g.getCentroid
    try {
      val utmCrs = UTM.getZoneCrs(centroid.getX, centroid.getY)
      g.reproject(LatLng, utmCrs)
    } catch {
      case e: IllegalArgumentException =>
        //logError(s"Failed to find UTM zone for $g; Returning LatLng geometry")
        g
      case e: org.locationtech.proj4j.UnknownAuthorityCodeException =>
        //logError(s"Encountered invalid UTM zone for $g; Returning LatLng geometry")
        g
      case e: IllegalStateException =>
        g
    }
  }

  @deprecated("Prefer withLinearDelta", "0.1.0")
  def withDelta(df: DataFrame): DataFrame = addLinearDelta(df).withColumnRenamed("linearDelta", "delta")

  def addLinearDelta(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    df.withColumn("linearDelta",
      when(isLinear('tags),
        abs(
          coalesce(when(st_geometryType('geom) === "LineString", st_lengthSphere(st_castToLineString('geom))), lit(0)) -
            coalesce(when(st_geometryType('prevGeom) === "LineString", st_lengthSphere(st_castToLineString('prevGeom))), lit(0))
        ))
        .otherwise(lit(0)))
  }

  def addAreaDelta(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    df
      .withColumn("geomUtm", transformLatLngToUtm('geom))
      .withColumn("prevGeomUtm", transformLatLngToUtm('prevGeom))
      .withColumn("areaDelta", abs(
        coalesce(when(!isnull('geom) and st_isValid('geom), st_area('geomUtm)), lit(0)) -
        coalesce(when(!isnull('prevGeom) and st_isValid('prevGeom), st_area('prevGeomUtm)), lit(0)))
      )
      .drop("geomUtm", "prevGeomUtm")
  }

  def addPrevGeom(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

    df.withColumn("prevGeom", lag('geom, 1) over idByUpdated)
  }

  implicit class DataFrameHelper(df: DataFrame) {
    def withAreaDelta: DataFrame = addAreaDelta(df)

    def withLinearDelta: DataFrame = addLinearDelta(df)

    def withPrevGeom: DataFrame = addPrevGeom(df)
  }

  def isInteresting(tags: Column): Column = isInterestingNode(tags) or isInterestingWay(tags)

  def isInterestingNode(tags: Column): Column = isPOI(tags) as 'isInterestingNode

  def isInterestingWay(tags: Column): Column =
    isBuilding(tags) or
    isRoad(tags) or
    isWaterway(tags) or
    isCoastline(tags) or
    isPOI(tags) as 'isInterestingWay

  def isLanduse(tags: Column): Column =
    tags.getItem("landuse").isNotNull as 'isLanduse

  def isNatural(tags: Column): Column =
    tags.getItem("natural").isNotNull as 'isNatural

  // Does this feature represent a rail-related site or area (not track)
  def isRailFeature(tags: Column): Column =
    array_contains(splitDelimitedValues(tags.getItem("railway")), "station") or
    array_contains(splitDelimitedValues(tags.getItem("railway")), "yard") or
    array_contains(splitDelimitedValues(tags.getItem("landuse")), "railway") as 'isRailSite

  // Does this feature represent a section of rail track
  def isRailLine(tags: Column): Column = not(isRailFeature(tags)) and tags.getItem("railway").isNotNull as 'isRailLine

  // Does this feature represent a rail-related entity
  def isRailway(tags: Column): Column =
    tags.getItem("railway").isNotNull or array_contains(splitDelimitedValues(tags.getItem("landuse")), "railway") as 'isRailway

  def isLinear(tags: Column): Column = isRoad(tags) or isWaterway(tags) or isCoastline(tags) or isRailLine(tags) as 'isLinear

  def isOther(tags: Column): Column = isTagged(tags) and
    not(isRoad((tags))) and
    not(isWaterway(tags)) and
    not(isBuilding(tags)) and
    not(isRailway(tags)) and
    not(isNatural(tags)) and
    not(isLanduse(tags)) and
    not(isPOI(tags)) as 'isOther

  def DefaultMeasurements(implicit sparkSession: SparkSession): Column = {
    import sparkSession.implicits._

    simplify_measurements(map(
      lit("road_km_added"), (isRoad('tags) and isNew('version, 'minorVersion)).cast(IntegerType) * 'linearDelta / 1000,
      lit("road_km_modified"), (isRoad('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType) * 'linearDelta / 1000,
      lit("road_km_deleted"), (isRoad('tags) and !'visible).cast(IntegerType) * 'linearDelta / 1000,
      lit("waterway_km_added"), (isWaterway('tags) and isNew('version, 'minorVersion)).cast(IntegerType) * 'linearDelta / 1000,
      lit("waterway_km_modified"), (isWaterway('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType) * 'linearDelta / 1000,
      lit("waterway_km_deleted"), (isWaterway('tags) and !'visible).cast(IntegerType) * 'linearDelta / 1000,
      lit("coastline_km_added"), (isCoastline('tags) and isNew('version, 'minorVersion)).cast(IntegerType) * 'linearDelta / 1000,
      lit("coastline_km_modified"), (isCoastline('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType) * 'linearDelta / 1000,
      lit("coastline_km_deleted"), (isCoastline('tags) and !'visible).cast(IntegerType) * 'linearDelta / 1000,
      lit("railline_km_added"), (isRailLine('tags) and isNew('version, 'minorVersion)).cast(IntegerType) * 'linearDelta / 1000,
      lit("railline_km_modified"), (isRailLine('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType) * 'linearDelta / 1000,
      lit("railline_km_deleted"), (isRailLine('tags) and !'visible).cast(IntegerType) * 'linearDelta / 1000,
      lit("landuse_km2_added"), (isLanduse('tags) and isNew('version, 'minorVersion)).cast(IntegerType) * 'areaDelta / 1000 / 1000,
      lit("landuse_km2_modified"), (isLanduse('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType) * 'areaDelta / 1000 / 1000,
      lit("landuse_km2_deleted"), (isLanduse('tags) and !'visible).cast(IntegerType) * 'areaDelta / 1000 / 1000,
      lit("natural_km2_added"), (isNatural('tags) and isNew('version, 'minorVersion)).cast(IntegerType) * 'areaDelta / 1000 / 1000,
      lit("natural_km2_modified"), (isNatural('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType) * 'areaDelta / 1000 / 1000,
      lit("natural_km2_deleted"), (isNatural('tags) and !'visible).cast(IntegerType) * 'areaDelta / 1000 / 1000
    )) as 'measurements
  }

  def DefaultCounts(implicit sparkSession: SparkSession): Column = {
    import sparkSession.implicits._

    simplify_counts(map(
      lit("roads_added"), (isRoad('tags) and isNew('version, 'minorVersion)).cast(IntegerType),
      lit("roads_modified"), (isRoad('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType),
      lit("roads_deleted"), (isRoad('tags) and !'visible).cast(IntegerType),
      lit("waterways_added"), (isWaterway('tags) and isNew('version, 'minorVersion)).cast(IntegerType),
      lit("waterways_modified"), (isWaterway('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType),
      lit("waterways_deleted"), (isWaterway('tags) and !'visible).cast(IntegerType),
      lit("coastlines_added"), (isCoastline('tags) and isNew('version, 'minorVersion)).cast(IntegerType),
      lit("coastlines_modified"), (isCoastline('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType),
      lit("coastlines_deleted"), (isCoastline('tags) and !'visible).cast(IntegerType),
      lit("buildings_added"), (isBuilding('tags) and isNew('version, 'minorVersion)).cast(IntegerType),
      lit("buildings_modified"), (isBuilding('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType),
      lit("buildings_deleted"), (isBuilding('tags) and !'visible).cast(IntegerType),
      lit("railway_features_added"), (isRailFeature('tags) and isNew('version, 'minorVersion)).cast(IntegerType),
      lit("railway_features_modified"), (isRailFeature('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType),
      lit("railway_features_deleted"), (isRailFeature('tags) and !'visible).cast(IntegerType),
      lit("raillines_added"), (isRailLine('tags) and isNew('version, 'minorVersion)).cast(IntegerType),
      lit("raillines_modified"), (isRailLine('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType),
      lit("raillines_deleted"), (isRailLine('tags) and !'visible).cast(IntegerType),
      lit("pois_added"), (isPOI('tags) and isNew('version, 'minorVersion)).cast(IntegerType),
      lit("pois_modified"), (isPOI('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType),
      lit("pois_deleted"), (isPOI('tags) and !'visible).cast(IntegerType),
      lit("landuse_added"), (isLanduse('tags) and isNew('version, 'minorVersion)).cast(IntegerType),
      lit("landuse_modified"), (isLanduse('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType),
      lit("landuse_deleted"), (isLanduse('tags) and !'visible).cast(IntegerType),
      lit("natural_added"), (isNatural('tags) and isNew('version, 'minorVersion)).cast(IntegerType),
      lit("natural_modified"), (isNatural('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType),
      lit("natural_deleted"), (isNatural('tags) and !'visible).cast(IntegerType),
      lit("other_added"), (isOther('tags) and isNew('version, 'minorVersion)).cast(IntegerType),
      lit("other_modified"), (isOther('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType),
      lit("other_deleted"), (isOther('tags) and !'visible).cast(IntegerType)
    )) as 'counts
  }
}
