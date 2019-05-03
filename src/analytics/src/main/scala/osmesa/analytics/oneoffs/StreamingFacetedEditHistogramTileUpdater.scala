package osmesa.analytics.oneoffs

import java.io.File
import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.locationtech.geomesa.spark.jts._
import osmesa.analytics.{Analytics, EditHistogram}
import osmesa.common.ProcessOSM.{NodeType, WayType}
import osmesa.common.functions.osm._
import osmesa.common.sources.Source

object StreamingFacetedEditHistogramTileUpdater
    extends CommandApp(
      name = "faceted-edit-histogram-tile-updater",
      header = "Update vector tiles containing faceted histograms of editing activity",
      main = {
        val augmentedDiffSourceOpt = Opts.option[URI](
          "augmented-diff-source",
          short = "a",
          metavar = "uri",
          help = "Location of augmented diffs to process"
        )

        val startSequenceOpt = Opts
          .option[Int](
            "start-sequence",
            short = "s",
            metavar = "sequence",
            help = "Starting sequence. If absent, the current (remote) sequence will be used."
          )
          .orNone

        val batchSizeOpt = Opts
          .option[Int]("batch-size",
                       short = "b",
                       metavar = "batch size",
                       help = "Change batch size.")
          .orNone

        val tileSourceOpt = Opts
          .option[URI](
            "tile-source",
            short = "t",
            metavar = "uri",
            help = "URI prefix of MVTs to update"
          )
          .withDefault(new File("").toURI)

        val concurrentUploadsOpt = Opts
          .option[Int]("concurrent-uploads",
                       short = "c",
                       metavar = "concurrent uploads",
                       help = "Set the number of concurrent uploads.")
          .orNone

        val databaseUrlOpt =
          Opts
            .option[URI](
              "database-url",
              short = "d",
              metavar = "database URL",
              help = "Database URL (default: DATABASE_URL environment variable)"
            )
            .orElse(Opts.env[URI]("DATABASE_URL", help = "The URL of the database"))
            .orNone

        val baseZoomOpt = Opts
          .option[Int]("base-zoom",
                       short = "z",
                       metavar = "Base zoom",
                       help = "Most detailed zoom level")
          .orNone

        (augmentedDiffSourceOpt,
         startSequenceOpt,
         batchSizeOpt,
         tileSourceOpt,
         concurrentUploadsOpt,
         databaseUrlOpt,
         baseZoomOpt)
          .mapN {
            (augmentedDiffSource,
             startSequence,
             batchSize,
             tileSource,
             _concurrentUploads,
             databaseUrl,
             baseZoom) =>
              val AppName = "FactedEditHistogramTileUpdater"

              implicit val spark: SparkSession =
                Analytics.sparkSession("State of the Data faceted tile generation")
              import spark.implicits._
              implicit val concurrentUploads: Option[Int] = _concurrentUploads
              spark.withJTS

              val options = Map(Source.BaseURI -> augmentedDiffSource.toString,
                                Source.ProcessName -> AppName) ++
                databaseUrl
                  .map(x => Map(Source.DatabaseURI -> x.toString))
                  .getOrElse(Map.empty) ++
                startSequence
                  .map(x => Map(Source.StartSequence -> x.toString))
                  .getOrElse(Map.empty) ++
                batchSize
                  .map(x => Map(Source.BatchSize -> x.toString))
                  .getOrElse(Map.empty)

              val diffs = spark.readStream
                .format(Source.AugmentedDiffs)
                .options(options)
                .load
                // convert sequence into timestamp
                .withColumn("watermark", to_timestamp(from_unixtime('sequence * 60 + 1347432900)))
                .withWatermark("watermark", "0 seconds")

              val nodes = diffs
                .select(
                  'watermark,
                  'sequence,
                  'id,
                  'version,
                  'updated,
                  when('visible, 'tags).otherwise('prevTags) as 'tags,
                  'geom,
                  'visible
                )
                .where('type === lit(NodeType))

              val ways = diffs
                .select(
                  'watermark,
                  'sequence,
                  'id,
                  'updated,
                  'nds,
                  when('visible, 'tags).otherwise('prevTags) as 'tags,
                  'visible
                )
                .where('type === lit(WayType) and 'minorVersion === 0)

              val wayTags = diffs
                .select(
                  'watermark,
                  'sequence,
                  'id,
                  explode('nds) as 'ref,
                  when('visible, 'tags).otherwise('prevTags) as 'tags
                )
                .where('type === lit(WayType))

              // major versions of nodes
              val majorVersions = nodes
                .withColumnRenamed("id", "ref")
                .join(wayTags.select('watermark, 'sequence, 'ref, 'tags as 'wayTags),
                      Seq("sequence", "ref", "watermark"),
                      "left_outer")
                .select(
                  'watermark,
                  'sequence,
                  'ref as 'id,
                  'version,
                  lit(true) as 'geometryChanged,
                  'geom,
                  'updated,
                  'tags,
                  coalesce('wayTags, map()) as 'wayTags,
                  mergeTags('tags, coalesce('wayTags, map())) as 'mergedTags,
                  'visible
                )

              // minor versions of nodes (without node tags, as they haven't been touched)
              val minorVersions = ways
                .select('watermark, 'sequence, 'updated, explode('nds) as 'ref, 'tags)
                .join(nodes.select('sequence, 'id as 'ref, 'version, 'geom, 'visible),
                      Seq("sequence", "ref"))
                .select(
                  'watermark,
                  'sequence,
                  'ref as 'id,
                  'version,
                  lit(true) as 'geometryChanged,
                  'geom,
                  'updated,
                  map() as 'tags,
                  'tags as 'wayTags,
                  'tags as 'mergedTags,
                  'visible
                )

              val processedNodes = majorVersions
                .union(minorVersions)
                .groupBy(
                  'watermark,
                  'sequence,
                  'id,
                  'version,
                  'updated
                )
                .agg(
                  first('geom) as 'geom,
                  first('visible) as 'visible,
                  sum('geometryChanged.cast(IntegerType)) > 0 as 'geometryChanged,
                  // combine tags together, joining unique values with ;s
                  // TODO better as a UDAF to avoid intermediate duplicates
                  reduceTags(collect_list('tags)) as 'tags,
                  reduceTags(collect_list('wayTags)) as 'wayTags,
                  reduceTags(collect_list('mergedTags)) as 'mergedTags
                )

              // a side-effect of tracking way tag changes is that way modifications touch all nodes
              // these can be identified by looking for minorVersion and should only be accounted for when tracking
              // specific feature types
              // should these be treated with lower weights?

              // in terms of raw data, how is this different from what's measured in the stats components?
              // a) no lengths
              // b) counts nodes, not ways (good for showing where, when gridded)
              val points = processedNodes
                .where('geom.isNotNull)
                .select(
                  'sequence,
                  'geom,
                  year('updated) * 1000 + dayofyear('updated) as 'key,
                  map(
                    lit("building"),
                    isBuilding('mergedTags).cast(IntegerType),
                    lit("road"),
                    isRoad('wayTags).cast(IntegerType),
                    lit("waterway"),
                    isWaterway('wayTags).cast(IntegerType),
                    lit("poi"),
                    isPOI('mergedTags).cast(IntegerType),
                    lit("coastline"),
                    isCoastline('wayTags).cast(IntegerType),
                    lit("created"),
                    ('version === 1 and 'geometryChanged).cast(IntegerType),
                    lit("modified"),
                    ('version > 1).cast(IntegerType),
                    lit("deleted"),
                    (!'visible).cast(IntegerType),
                    lit("metadataOnly"),
                    (!'geometryChanged).cast(IntegerType)
                  ) as 'facets
                )

              val tiledNodes =
                EditHistogram.update(points,
                                     tileSource,
                                     baseZoom.getOrElse(EditHistogram.DefaultBaseZoom))

              val query = tiledNodes.writeStream
                .queryName("faceted edit histogram tile updates")
                .format("console")
                .start

              query.awaitTermination()

              spark.stop()
          }
      }
    )
