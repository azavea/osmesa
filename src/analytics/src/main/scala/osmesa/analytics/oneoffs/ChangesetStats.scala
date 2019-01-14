package osmesa.analytics.oneoffs

import java.net.URI

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.locationtech.geomesa.spark.jts.st_length
import osmesa.analytics.Analytics
import osmesa.common.ProcessOSM
import osmesa.common.functions._
import osmesa.common.functions.osm._

object ChangesetStats extends CommandApp(
  name = "changeset-stats",
  header = "Changeset statistics",
  main = {
    val historyOpt =
      Opts.option[String]("history", help = "Location of the History ORC file to process.")
    val changesetsOpt =
      Opts.option[String]("changesets", help = "Location of the Changesets ORC file to process.")
    val outputOpt =
      Opts.option[URI](long = "output", help = "Output URI prefix; trailing / must be included")

    (historyOpt, changesetsOpt, outputOpt).mapN { (historySource, changesetSource, output) =>
      implicit val spark: SparkSession = Analytics.sparkSession("ChangesetStats")
      import spark.implicits._

      val history = spark.read.orc(historySource)

      val pointGeoms = ProcessOSM.geocode(ProcessOSM.constructPointGeometries(
        // pre-filter to POI nodes
        history.where('type === "node" and isPOI('tags))
      ).withColumn("minorVersion", lit(0)))

      val wayGeoms = ProcessOSM.geocode(ProcessOSM.reconstructWayGeometries(
        // pre-filter to interesting ways
        history.where('type === "way" and (isBuilding('tags) or isRoad('tags) or isWaterway('tags) or isCoastline('tags) or isPOI('tags))),
        // let reconstructWayGeometries do its thing; nodes are cheap
        history.where('type === "node")
      ).drop('geometryChanged))

      @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

      val augmentedWays = wayGeoms.withColumn("length", st_length('geom))
        .withColumn("delta",
          when(isRoad('tags) or isWaterway('tags) or isCoastline('tags),
            coalesce(abs('length - (lag('length, 1) over idByUpdated)), lit(0)))
            .otherwise(lit(0)))

      val wayChangesetStats = augmentedWays
        .withColumn("road_m_added",
          when(isRoad('tags) and isNew('version, 'minorVersion), 'length)
            .otherwise(lit(0)))
        .withColumn("road_m_modified",
          when(isRoad('tags) and not(isNew('version, 'minorVersion)), 'delta)
            .otherwise(lit(0)))
        .withColumn("waterway_m_added",
          when(isWaterway('tags) and isNew('version, 'minorVersion), 'length)
            .otherwise(lit(0)))
        .withColumn("waterway_m_modified",
          when(isWaterway('tags) and not(isNew('version, 'minorVersion)), 'delta)
            .otherwise(lit(0)))
        .withColumn("coastline_m_added",
          when(isCoastline('tags) and isNew('version, 'minorVersion), 'length)
            .otherwise(lit(0)))
        .withColumn("coastline_m_modified",
          when(isCoastline('tags) and not(isNew('version, 'minorVersion)), 'delta)
            .otherwise(lit(0)))
        .withColumn("roads_added",
          when(isRoad('tags) and isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)))
        .withColumn("roads_modified",
          when(isRoad('tags) and not(isNew('version, 'minorVersion)), lit(1))
            .otherwise(lit(0)))
        .withColumn("waterways_added",
          when(isWaterway('tags) and isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)))
        .withColumn("waterways_modified",
          when(isWaterway('tags) and not(isNew('version, 'minorVersion)), lit(1))
            .otherwise(lit(0)))
        .withColumn("coastlines_added",
          when(isCoastline('tags) and isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)))
        .withColumn("coastlines_modified",
          when(isCoastline('tags) and not(isNew('version, 'minorVersion)), lit(1))
            .otherwise(lit(0)))
        .withColumn("buildings_added",
          when(isBuilding('tags) and isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)))
        .withColumn("buildings_modified",
          when(isBuilding('tags) and not(isNew('version, 'minorVersion)), lit(1))
            .otherwise(lit(0)))
        .withColumn("pois_added",
          when(isPOI('tags) and isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)))
        .withColumn("pois_modified",
          when(isPOI('tags) and not(isNew('version, 'minorVersion)), lit(1))
            .otherwise(lit(0)))
        .groupBy('changeset)
        .agg(
          sum('road_m_added / 1000).as('road_km_added),
          sum('road_m_modified / 1000).as('road_km_modified),
          sum('waterway_m_added / 1000).as('waterway_km_added),
          sum('waterway_m_modified / 1000).as('waterway_km_modified),
          sum('coastline_m_added / 1000).as('coastline_km_added),
          sum('coastline_m_modified / 1000).as('coastline_km_modified),
          sum('roads_added).as('roads_added),
          sum('roads_modified).as('roads_modified),
          sum('waterways_added).as('waterways_added),
          sum('waterways_modified).as('waterways_modified),
          sum('coastlines_added).as('coastlines_added),
          sum('coastlines_modified).as('coastlines_modified),
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
        .withColumn("coastline_km_added", coalesce('coastline_km_added, lit(0)))
        .withColumn("coastline_km_modified", coalesce('coastline_km_modified, lit(0)))
        .withColumn("roads_added", coalesce('roads_added, lit(0)))
        .withColumn("roads_modified", coalesce('roads_modified, lit(0)))
        .withColumn("waterways_added", coalesce('waterways_added, lit(0)))
        .withColumn("waterways_modified", coalesce('waterways_modified, lit(0)))
        .withColumn("coastlines_added", coalesce('coastlines_added, lit(0)))
        .withColumn("coastlines_modified", coalesce('coastlines_modified, lit(0)))
        .withColumn("buildings_added", coalesce('buildings_added, lit(0)))
        .withColumn("buildings_modified", coalesce('buildings_modified, lit(0)))
        .withColumn("pois_added",
          coalesce('way_pois_added, lit(0)) + coalesce('node_pois_added, lit(0)))
        .withColumn("pois_modified",
          coalesce('way_pois_modified, lit(0)) + coalesce('node_pois_modified, lit(0)))
        .withColumn("countries", merge_counts('node_countries, 'way_countries))
        .drop('way_pois_added)
        .drop('node_pois_added)
        .drop('way_pois_modified)
        .drop('node_pois_modified)
        .drop('way_countries)
        .drop('node_countries)

       val changesets = spark.read.orc(changesetSource)

      val changesetMetadata = changesets
        .select(
          'id as 'changeset,
          'uid,
          'user as 'name,
          'tags.getItem("created_by") as 'editor,
          'created_at,
          'closed_at,
          hashtags('tags.getField("comment")) as 'hashtags
        )

      val changesetStats = rawChangesetStats
        .join(changesetMetadata, Seq("changeset"), "left_outer")
        .withColumnRenamed("changeset", "id")
        .repartition(1)
        .cache

      val changesetsCountriesTable = changesetStats
        .select('id as 'changeset_id, explode('countries) as Seq("country", "edit_count"))

      val changesetsHashtagsTable = changesetStats
        .select('id as 'changeset_id, explode('hashtags) as 'hashtag)

      val usersTable = changesetStats
        .select('uid as 'id, 'name)
        .distinct

      val changesetsTable = changesetStats
        .withColumnRenamed("uid", "user_id")
        .drop('countries)
        .drop('hashtags)
        .drop('name)

      changesetsTable
        .write
        .mode(SaveMode.Overwrite)
        .csv(output.resolve("changesets").toString)

      changesetsCountriesTable
        .write
        .mode(SaveMode.Overwrite)
        .csv(output.resolve("changesets_countries").toString)

      changesetsHashtagsTable
        .write
        .mode(SaveMode.Overwrite)
        .csv(output.resolve("changesets_hashtags").toString)

      usersTable
        .write
        .mode(SaveMode.Overwrite)
        .csv(output.resolve("users").toString)

      spark.stop()
    }
  }
)
