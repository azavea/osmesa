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
import osmesa.common.sources.Source

object CoastlineBackfillJob extends CommandApp(
  name = "coastline-backfill",
  header = "Generate coastline stats to merge into existing tables",
  main = {
    val historyOpt =
      Opts.option[String]("history", help = "Location of the History ORC file to process")
    val changesetsOpt =
      Opts.option[String]("changesets", help = "Location of the Changesets ORC file to process")
    val outputOpt =
      Opts.option[URI](long = "output", help = "Output URI prefix; trailing / must be included")

    (historyOpt, changesetsOpt, outputOpt).mapN { (historySource, changesetSource, output) =>
      implicit val spark: SparkSession = Analytics.sparkSession("ChangesetStats")
      import spark.implicits._

      val history = spark.read.orc(historySource)

      val wayGeoms = ProcessOSM.geocode(ProcessOSM.reconstructWayGeometries(
        // pre-filter to interesting ways
        history.where('type === "way" and isCoastline('tags)),
        // let reconstructWayGeometries do its thing; nodes are cheap
        history.where('type === "node")
      ).drop('geometryChanged))

      @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

      val augmentedWays = wayGeoms.withColumn("length", st_length('geom))
        .withColumn("delta",
          when(isCoastline('tags),
            coalesce(abs('length - (lag('length, 1) over idByUpdated)), lit(0)))
            .otherwise(lit(0)))

      val wayChangesetStats = augmentedWays
        .withColumn("coastline_m_added",
          when(isCoastline('tags) and isNew('version, 'minorVersion), 'length)
            .otherwise(lit(0)))
        .withColumn("coastline_m_modified",
          when(isCoastline('tags) and not(isNew('version, 'minorVersion)), 'delta)
            .otherwise(lit(0)))
        .withColumn("coastlines_added",
          when(isCoastline('tags) and isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)))
        .withColumn("coastlines_modified",
          when(isCoastline('tags) and not(isNew('version, 'minorVersion)), lit(1))
            .otherwise(lit(0)))
        .groupBy('changeset)
        .agg(
          sum('coastline_m_added / 1000).as('coastline_km_added),
          sum('coastline_m_modified / 1000).as('coastline_km_modified),
          sum('coastlines_added).as('coastlines_added),
          sum('coastlines_modified).as('coastlines_modified),
          count_values(flatten(collect_list('countries))) as 'countries
        )

      // coalesce values to deal with nulls introduced by the outer join
      val rawChangesetStats = wayChangesetStats
        .withColumn("coastline_km_added", coalesce('coastline_km_added, lit(0)))
        .withColumn("coastline_km_modified", coalesce('coastline_km_modified, lit(0)))
        .withColumn("coastlines_added", coalesce('coastlines_added, lit(0)))
        .withColumn("coastlines_modified", coalesce('coastlines_modified, lit(0)))

      // val changesets = spark.read
      //                       .format("changesets")
      //                       .option(Source.BaseURI, "http://10.0.1.244/replication/replication/changesets/")
      //                       .option(Source.ProcessName, "CoastlineBackfillGenerator")
      //                       .option(Source.StartSequence, startSequence.toString)
      //                       .load

      // val changesetMetadata = changesets
      //   .select(
      //     'id as 'changeset,
      //     'uid,
      //     'user as 'name,
      //     'tags.getItem("created_by") as 'editor,
      //     'createdAt,
      //     'closedAt,
      //     hashtags('tags) as 'hashtags
      //   )

      // val changesetStats = rawChangesetStats
      //   .join(changesetMetadata, Seq("changeset"), "left_outer")

      // changesetStats
      //   .repartition(50)
      //   .write
      //   .mode(SaveMode.Overwrite)
      //   .orc(output.resolve("changesets").toString)

      rawChangesetStats
        .repartition(50)
        .write
        .mode(SaveMode.Overwrite)
        .orc(output.resolve("coastline-backfill.orc").toString)

      spark.stop()
    }
  }
)
