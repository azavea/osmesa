package osmesa.apps.batch

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.locationtech.geomesa.spark.jts._
import osmesa.analytics.{Analytics, EditHistogram}
import vectorpipe.functions.osm._
import vectorpipe.{internal => ProcessOSM}

object FacetedEditHistogramTileCreator
    extends CommandApp(
      name = "faceted-edit-histogram-tiler-creator",
      header = "Create vector tiles containing faceted histograms of editing activity",
      main = {

        val historyOpt = Opts
          .option[URI]("history", help = "URI of the history ORC file to process.")

        val outputOpt = Opts.option[URI]("out", help = "Base URI for output.")

        val concurrentUploadsOpt = Opts
          .option[Int]("concurrent-uploads",
                       short = "c",
                       metavar = "concurrent uploads",
                       help = "Set the number of concurrent uploads.")
          .orNone

        val baseZoomOpt = Opts
          .option[Int]("base-zoom",
                       short = "z",
                       metavar = "Base zoom",
                       help = "Most detailed zoom level")
          .orNone

        (
          historyOpt,
          outputOpt,
          concurrentUploadsOpt,
          baseZoomOpt
        ).mapN {
          (historyURI, outputURI, _concurrentUploads, baseZoom) =>
            implicit val spark: SparkSession =
              Analytics.sparkSession("State of the Data faceted tile generation")
            import spark.implicits._
            implicit val concurrentUploads: Option[Int] = _concurrentUploads
            spark.withJTS

            val history = spark.read
              .orc(historyURI.toString)

            // pre-processing requires that all elements be present in order to assign coordinates to deletes
            // subsequent processing can handle incomplete nds lists for ways since it's about transferring tags, not
            // assembling geometries
            val nodes = ProcessOSM.preprocessNodes(history)
              .where('uid > 1)
            val ways = ProcessOSM.preprocessWays(history)
              .where('uid > 1)

            // Associate nodes with way tags using a slightly modified version of the process for assembling way
            // geometries

            val nodesToWays =
              ways.select(explode('nds) as 'id, 'id as 'wayId, 'version, 'timestamp, 'validUntil)

            // Create a way entry for each changeset in which a node was modified, containing the timestamp of the node that
            // triggered the association.
            val waysByChangeset = nodes
              .select('changeset, 'id, 'timestamp as 'updated)
              .join(nodesToWays, Seq("id"))
              .where('timestamp <= 'updated and 'updated < coalesce('validUntil, current_timestamp))
              .select('changeset, 'wayId as 'id, 'version, 'updated)

            val allWayVersions = waysByChangeset
            // Union with raw ways to include those in the time line (if they weren't already triggered by node modifications
            // at the same time)
              .union(ways.select('changeset, 'id, 'version, 'timestamp as 'updated))
              // If a node and a way were modified within the same changeset at different times, there will be multiple entries
              // per changeset (with different timestamps). There should only be one per changeset.
              .groupBy('changeset, 'id)
              .agg(max('version).cast(IntegerType) as 'version, max('updated) as 'updated)
              // ProcessOSM difference: isArea is unused
              .join(ways.select('id, 'version, 'nds, 'tags), Seq("id", "version"))

            val explodedWays = allWayVersions
            // ProcessOSM difference: order doesn't matter, ways w/ empty nds aren't useful, isArea is unused
              .select('changeset, 'id, 'version, 'updated, 'tags, explode('nds) as 'ref)
              // repartition including updated timestamp to avoid skew (version is insufficient, as
              // multiple instances may exist with the same version)
              .repartition('id, 'updated)

            val nodesWithWayTags = explodedWays
            // ProcessOSM difference: right outer join is necessary
              .join(nodes.select('id as 'ref,
                                 'version as 'refVersion,
                                 'tags as 'refTags,
                                 'timestamp,
                                 'validUntil,
                                 'lat,
                                 'lon,
                                 'visible,
                                 'geometryChanged),
                    Seq("ref"), "right_outer")
              .where('updated.isNull or ('timestamp <= 'updated and 'updated < coalesce('validUntil, current_timestamp)))
              .drop('changeset)
              .drop('updated)
              .drop('validUntil)
              .withColumnRenamed("timestamp", "updated")

            @transient val idAndVersionByUpdated =
              Window.partitionBy('id, 'version).orderBy('updated)

            val processedNodes = nodesWithWayTags
              .groupBy('ref as 'id, 'refVersion as 'version, 'updated)
              .agg(
                reduceTags(collect_list('refTags)) as 'tags,
                first('lat) as 'lat,
                first('lon) as 'lon,
                first('visible) as 'visible,
                first('geometryChanged) as 'geometryChanged,
                // combine tags together, joining unique values with ;s
                // TODO better as a UDAF to avoid intermediate duplicates
                reduceTags(collect_list(coalesce('tags, map()))) as 'wayTags
              )
              // here, minor versions are nodes where only contributing metadata (i.e. from ways) has changed
              .withColumn("minorVersion", (row_number over idAndVersionByUpdated) - 1)
              .withColumn(
                "mergedTags",
                // combine node + way tags if the node is responsible for the change
                when('minorVersion === 0, mergeTags('tags, 'wayTags))
                // otherwise just use the way tags
                  .otherwise('wayTags)
              )

            // a side-effect of tracking way tag changes is that way modifications touch all nodes
            // these can be identified by looking for minorVersion and should only be accounted for when tracking
            // specific feature types
            // should these be treated with lower weights?

            // in terms of raw data, how is this different from what's measured in the stats components?
            // a) no lengths
            // b) counts nodes, not ways (good for showing where, when gridded)
            val points = processedNodes
              .where('lat =!= lit(Double.NaN) and 'lon =!= lit(Double.NaN))
              .select(
                st_makePoint('lon, 'lat) as 'geom,
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
                  isNew('version, 'minorVersion).cast(IntegerType),
                  lit("modified"),
                  ('visible and !isNew('version, 'minorVersion)).cast(IntegerType),
                  lit("deleted"),
                  (!'visible).cast(IntegerType),
                  lit("metadataOnly"),
                  ('minorVersion > 0 or !'geometryChanged).cast(IntegerType)
                ) as 'facets
              )

            // Aggregated statistics by day for the last year:
            // points.groupBy('key).agg(sum_counts(collect_list('facets)) as 'counts).orderBy('key desc).show(365, false)

            val stats = EditHistogram.create(points,
                                             outputURI,
                                             baseZoom.getOrElse(EditHistogram.DefaultBaseZoom))
            println(s"${stats.count} tiles created.")

            spark.stop()
        }
      }
    )
