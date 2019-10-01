package osmesa.analytics

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._
import osmesa.analytics.stats.functions._
import vectorpipe.functions.osm._
import org.locationtech.geomesa.spark.jts._

package object stats {
  def addDelta(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    df.withColumn("delta",
      when(isLinear('tags),
        abs(
          coalesce(when(st_geometryType('geom) === "LineString", st_lengthSphere(st_castToLineString('geom))), lit(0)) -
            coalesce(when(st_geometryType('prevGeom) === "LineString", st_lengthSphere(st_castToLineString('prevGeom))), lit(0))
        ))
        .otherwise(lit(0)))
  }

  def addPrevGeom(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

    df.withColumn("prevGeom", lag('geom, 1) over idByUpdated)
  }

  implicit class DataFrameHelper(df: DataFrame) {
    def withDelta: DataFrame = addDelta(df)

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
    not(isCoastline(tags)) and
    not(isBuilding(tags)) and
    not(isRailway(tags)) and
    not(isPOI(tags)) as 'isOther

  def DefaultMeasurements(implicit sparkSession: SparkSession): Column = {
    import sparkSession.implicits._

    simplify_measurements(map(
      lit("road_km_added"), (isRoad('tags) and isNew('version, 'minorVersion)).cast(IntegerType) * 'delta / 1000,
      lit("road_km_modified"), (isRoad('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType) * 'delta / 1000,
      lit("road_km_deleted"), (isRoad('tags) and !'visible).cast(IntegerType) * 'delta / 1000,
      lit("waterway_km_added"), (isWaterway('tags) and isNew('version, 'minorVersion)).cast(IntegerType) * 'delta / 1000,
      lit("waterway_km_modified"), (isWaterway('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType) * 'delta / 1000,
      lit("waterway_km_deleted"), (isWaterway('tags) and !'visible).cast(IntegerType) * 'delta / 1000,
      lit("coastline_km_added"), (isCoastline('tags) and isNew('version, 'minorVersion)).cast(IntegerType) * 'delta / 1000,
      lit("coastline_km_modified"), (isCoastline('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType) * 'delta / 1000,
      lit("coastline_km_deleted"), (isCoastline('tags) and !'visible).cast(IntegerType) * 'delta / 1000,
      lit("railline_km_added"), (isRailLine('tags) and isNew('version, 'minorVersion)).cast(IntegerType) * 'delta / 1000,
      lit("railline_km_modified"), (isRailLine('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType) * 'delta / 1000,
      lit("railline_km_deleted"), (isRailLine('tags) and !'visible).cast(IntegerType) * 'delta / 1000
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
      lit("other_added"), (isOther('tags) and isNew('version, 'minorVersion)).cast(IntegerType),
      lit("other_modified"), (isOther('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType),
      lit("other_deleted"), (isOther('tags) and !'visible).cast(IntegerType)
    )) as 'counts
  }

  def pointCounts(implicit sparkSession: SparkSession): Column = {
    import sparkSession.implicits._

    simplify_counts(map(
      lit("pois_added"), (isPOI('tags) and 'version === 1).cast(IntegerType),
      lit("pois_modified"), (isPOI('tags) and 'version > 1 and 'visible).cast(IntegerType),
      lit("pois_deleted"), (isPOI('tags) and !'visible).cast(IntegerType),
      lit("other_added"), (isOther('tags) and isNew('version, 'minorVersion)).cast(IntegerType),
      lit("other_modified"), (isOther('tags) and not(isNew('version, 'minorVersion)) and 'visible).cast(IntegerType),
      lit("other_deleted"), (isOther('tags) and !'visible).cast(IntegerType)
    )) as 'counts
  }
}
