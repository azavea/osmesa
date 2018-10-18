package osmesa.analytics.oneoffs

import java.net.URI

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import osmesa.analytics.Analytics
import osmesa.common.ProcessOSM
import osmesa.common.functions._
import osmesa.common.functions.osm._
import org.locationtech.geomesa.spark.jts._


object CentroidStats extends CommandApp(
  name = "centroid-stats",
  header = "centroid statistics",
  main = {
    val historyOpt =
      Opts.option[String]("history", help = "Location of the History ORC file to process.")
    val outputOpt =
      Opts.option[URI](long = "output", help = "Output URI prefix; trailing / must be included")

    (historyOpt, outputOpt).mapN { (historySource, output) =>
      implicit val spark: SparkSession = Analytics.sparkSession("ChangesetStats").withJTS
      import spark.implicits._

      val history = spark.read.orc(historySource)

      val pointGeoms = ProcessOSM.geocode(ProcessOSM.constructPointGeometries(
        // pre-filter to POI nodes
        history.where('type === "node" and isPOI('tags))
      ).withColumn("minorVersion", lit(0)))

      val wayGeoms = ProcessOSM.geocode(ProcessOSM.reconstructWayGeometries(
        // pre-filter to interesting ways
        history.where('type === "way" and (isBuilding('tags) or isRoad('tags) or isWaterway('tags) or isPOI('tags))),
        // let reconstructWayGeometries do its thing; nodes are cheap
        history.where('type === "node")
      ).drop('geometryChanged))

      @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

      val augmentedWays = wayGeoms
        .withColumn("length", st_length('geom))
        .withColumn("delta",
          when(isRoad('tags) or isWaterway('tags),
            coalesce(abs('length - (lag('length, 1) over idByUpdated)), lit(0)))
            .otherwise(lit(0)))

      val wayChangesetStats = augmentedWays
        .withColumn("road_m_added",
          when(isRoad('tags) and 'version === 1 and 'minorVersion === 0, 'length)
            .otherwise(lit(0)))
        .withColumn("road_m_modified",
          when(isRoad('tags) and not('version === 1 and 'minorVersion === 0), 'delta)
            .otherwise(lit(0)))
        .withColumn("waterway_m_added",
          when(isWaterway('tags) and 'version === 1 and 'minorVersion === 0, 'length)
            .otherwise(lit(0)))
        .withColumn("waterway_m_modified",
          when(isWaterway('tags) and not('version === 1 and 'minorVersion === 0), 'delta)
            .otherwise(lit(0)))
        .withColumn("roads_added",
          when(isRoad('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("roads_modified",
          when(isRoad('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("waterways_added",
          when(isWaterway('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("waterways_modified",
          when(isWaterway('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("buildings_added",
          when(isBuilding('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("buildings_modified",
          when(isBuilding('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .withColumn("pois_added",
          when(isPOI('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
            .otherwise(lit(0)))
        .withColumn("pois_modified",
          when(isPOI('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
            .otherwise(lit(0)))
        .groupBy('changeset)
        .agg(
          sum('road_m_added / 1000).as('road_km_added),
          sum('road_m_modified / 1000).as('road_km_modified),
          sum('waterway_m_added / 1000).as('waterway_km_added),
          sum('waterway_m_modified / 1000).as('waterway_km_modified),
          sum('roads_added).as('roads_added),
          sum('roads_modified).as('roads_modified),
          sum('waterways_added).as('waterways_added),
          sum('waterways_modified).as('waterways_modified),
          sum('buildings_added).as('buildings_added),
          sum('buildings_modified).as('buildings_modified),
          sum('pois_added).as('pois_added),
          sum('pois_modified).as('pois_modified),
          count_values(flatten(collect_list('countries))) as 'countries
        )

      val pointChangesetStats = pointGeoms
        .withColumn("pois_added",
          when(isPOI('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("pois_modified",
          when(isPOI('tags) and 'version > 1, lit(1))
            .otherwise(lit(0)))
        .groupBy('changeset)
        .agg(
          sum('pois_added) as 'pois_added,
          sum('pois_modified) as 'pois_modified,
          count_values(flatten(collect_list('countries))) as 'countries
        )

      // coalesce values to deal with nulls introduced by the outer join
      val rawChangesetStats = wayChangesetStats
        .withColumnRenamed("pois_added", "way_pois_added")
        .withColumnRenamed("pois_modified", "way_pois_modified")
        .withColumnRenamed("countries", "way_countries")
        .join(pointChangesetStats
          .withColumnRenamed("pois_added", "node_pois_added")
          .withColumnRenamed("pois_modified", "node_pois_modified")
          .withColumnRenamed("countries", "node_countries"),
          Seq("changeset"),
          "full_outer")
        .withColumn("road_km_added", coalesce('road_km_added, lit(0)))
        .withColumn("road_km_modified", coalesce('road_km_modified, lit(0)))
        .withColumn("waterway_km_added", coalesce('waterway_km_added, lit(0)))
        .withColumn("waterway_km_modified", coalesce('waterway_km_modified, lit(0)))
        .withColumn("roads_added", coalesce('roads_added, lit(0)))
        .withColumn("roads_modified", coalesce('roads_modified, lit(0)))
        .withColumn("waterways_added", coalesce('waterways_added, lit(0)))
        .withColumn("waterways_modified", coalesce('waterways_modified, lit(0)))
        .withColumn("buildings_added", coalesce('buildings_added, lit(0)))
        .withColumn("buildings_modified", coalesce('buildings_modified, lit(0)))
        .withColumn("pois_added",
          coalesce('way_pois_added, lit(0)) + coalesce('node_pois_added, lit(0)))
        .withColumn("pois_modified",
          coalesce('way_pois_modified, lit(0)) + coalesce('node_pois_modified, lit(0)))
        .withColumn("countries", merge_counts('node_countries, 'way_countries))


      rawChangesetStats
        .repartition(50)
        .write
        .mode(SaveMode.Overwrite)
        .orc(output.resolve("centroid-stats").toString)

      spark.stop()
    }
  }
)

