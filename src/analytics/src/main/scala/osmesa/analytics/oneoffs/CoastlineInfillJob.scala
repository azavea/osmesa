package osmesa.analytics.oneoffs

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import geotrellis.vector.Feature
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.locationtech.geomesa.spark.jts._
import osmesa.analytics.Analytics
import osmesa.common.ProcessOSM
import osmesa.common.functions._
import osmesa.common.functions.osm._
import osmesa.common.model._
import osmesa.common.sources.Source

import java.io.File
import java.net.URI

/*
 * Usage example:
 *
 * sbt "project analytics" assembly
 *
 * spark-submit \
 *   --class osmesa.analytics.oneoffs.CoastlineInfillJob \
 *   src/analytics/target/scala-2.11/osmesa-analytics.jar
 */
object CoastlineInfillJob
    extends CommandApp(
      name = "osmesa-user-footprint-updater",
      header = "Consume minutely diffs + changesets and update coastline statistics",
      main = {
        val rootURI = new File("").toURI

        val augdiffSourceOpt = Opts
          .option[URI]("augdiff-source",
                       short = "a",
                       metavar = "uri",
                       help = "Location of augmented diffs to process")
        val augdiffStartSequenceOpt = Opts
          .option[Int](
            "augdiff-start-sequence",
            short = "s",
            metavar = "sequence",
            help =
              "Augmented diff starting sequence. If absent, the current (remote) sequence will be used.")
          .orNone
        val augdiffEndSequenceOpt = Opts
          .option[Int](
            "augdiff-end-sequence",
            short = "e",
            metavar = "sequence",
            help = "Augmented diff ending sequence. If absent, this will be an infinite stream.")
          .orNone
        val augdiffBatchSizeOpt = Opts
          .option[Int]("batch-size",
                       short = "b",
                       metavar = "batch size",
                       help = "Augdiff batch size.")
          .orNone
        val databaseUriOpt =
          Opts.option[URI](
            "database-uri",
            short = "d",
            metavar = "database URL",
            help = "Database URL (default: $DATABASE_URL environment variable)"
          )
        val databaseUriEnv =
          Opts.env[URI]("DATABASE_URL", help = "The URL of the database")
        val outputOpt =
          Opts.option[URI](long = "output", help = "Output URI prefix; trailing / must be included")

        (augdiffSourceOpt,
         augdiffStartSequenceOpt,
         augdiffEndSequenceOpt,
         augdiffBatchSizeOpt,
         databaseUriOpt,
         outputOpt).mapN {(augdiffSource,
                           augdiffStartSequence,
                           augdiffEndSequence,
                           augdiffBatchSize,
                           databaseUri,
                           output) =>
          val spark: SparkSession = Analytics.sparkSession("CoastlineInfill")
          import spark.implicits._

          val options =
            Map(
              Source.BaseURI -> augdiffSource.toString,
              Source.DatabaseURI -> databaseUri.toString,
              Source.ProcessName -> "CoastlineInfill") ++
            augdiffStartSequence.map(s => Map(Source.StartSequence -> s.toString)).getOrElse(Map.empty[String, String]) ++
            augdiffEndSequence.map(s => Map(Source.EndSequence -> s.toString)).getOrElse(Map.empty[String, String]) ++
            augdiffBatchSize.map(s => Map(Source.BatchSize -> s.toString)).getOrElse(Map.empty[String, String])

          val augdiffs = spark.read.format(Source.AugmentedDiffs).options(options).load

          @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

          val coastlines = augdiffs
            .filter(isCoastline('tags))
            .withColumn("length", st_length('geom))
            .withColumn("delta", coalesce(abs('length - (lag('length, 1) over idByUpdated)), lit(0)))

          val coastlineStats = coastlines
            .withColumn("coastline_m_added",
                        when(isNew('version, 'minorVersion), 'length).otherwise(lit(0)))
            .withColumn("coastline_m_modified",
                        when(not(isNew('version, 'minorVersion)), 'delta).otherwise(lit(0)))
            .withColumn("coastlines_added",
                        when(isNew('version, 'minorVersion), lit(1)).otherwise(lit(0)))
            .withColumn("coastlines_modified",
                        when(not(isNew('version, 'minorVersion)), lit(1)).otherwise(lit(0)))
            .groupBy('changeset)
            .agg(
              sum('coastline_m_added / 1000).as('coastline_km_added),
              sum('coastline_m_modified / 1000).as('coastline_km_modified),
              sum('coastlines_added).as('coastlines_added),
              sum('coastlines_modified).as('coastlines_modified)
            )

          coastlineStats
            .repartition(50)
            .write
            .mode(SaveMode.Overwrite)
            .orc(output.resolve("coastline-infill.orc").toString)

          spark.stop()
        }
      }
)
