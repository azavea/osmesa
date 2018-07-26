package osmesa

import java.sql.Timestamp
import java.time.Instant

import cats.implicits._
import com.monovore.decline._
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import osmesa.common.ProcessOSM
import osmesa.common.functions._
import osmesa.common.functions.osm._

/*
 * Usage example:
 *
 * sbt "project ingest" assembly
 *
 * spark-submit \
 *   --class osmesa.ExtractMultiPolygons \
 *   ingest/target/scala-2.11/osmesa-ingest.jar \
 *   --orc $HOME/data/manhattan.orc \
 *   --out $HOME/data/manhattan-building-relation-geoms
 */
object ExtractBuildingRelations
    extends CommandApp(
      name = "extract-building-relations",
      header = "Extract building relations from an ORC file",
      main = {

        /* CLI option handling */
        val orcO = Opts.option[String]("orc", help = "Location of the .orc file to process")
        val outGeomsO = Opts
          .option[String]("out", help = "Location of the ORC file to write containing geometries")
        val cacheDirO =
          Opts.option[String]("cache", help = "Location to cache ORC files").withDefault("")

        (orcO, outGeomsO, cacheDirO).mapN {
          (orc, outGeoms, cacheDir) =>
            /* Settings compatible with both local and EMR execution */
            val conf = new SparkConf()
              .setIfMissing("spark.master", "local[*]")
              .setAppName("extract-building-relations")
              .set("spark.serializer", classOf[org.apache.spark.serializer.KryoSerializer].getName)
              .set("spark.kryo.registrator",
                   classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)
              .set("spark.sql.orc.impl", "native")
              .set("spark.ui.showConsoleProgress", "true")

            implicit val ss: SparkSession = SparkSession.builder
              .config(conf)
              .enableHiveSupport
              .getOrCreate

            import ss.implicits._

            // quiet Spark
            Logger.getRootLogger.setLevel(Level.WARN)

            val df = ss.read.orc(orc)
            val nodes = df.where('type === "node")
            val ways = df.where('type === "way")

            // DOWN: get all versions of raw elements

            // get all building relations
            val relations = ProcessOSM.preprocessRelations(
              df.where('type === "relation" and isBuildingRelation('tags)))

            // get all nodes referenced by relations
            val nodeIds = relations
              .select(explode('members) as 'member)
              .where($"member.type" === ProcessOSM.NodeType)
              .select($"member.ref" as "id")
              .distinct

            val referencedNodes = ProcessOSM.preprocessNodes(nodes.join(nodeIds, Seq("id")))

            // get all ways referenced by relations
            val wayIds = relations
              .select(explode('members) as 'member)
              .where($"member.type" === ProcessOSM.WayType)
              .select($"member.ref" as "id")
              .distinct

            // get all nodes referenced by referenced ways

            val referencedWays = ProcessOSM.preprocessWays(
              ways
                .join(wayIds, Seq("id")))

            // create a lookup table for node → ways (using only the ways we'd previously identified)
            val nodesToWays = referencedWays
              .select(explode('nds) as 'id, 'id as 'wayId, 'version, 'timestamp)
              .distinct

            // extract the referenced nodes from the lookup table
            val wayNodeIds = nodesToWays
              .select('id)

            val referencedWayNodes = ProcessOSM.preprocessNodes(nodes.join(wayNodeIds, Seq("id")))

            // UP: assemble all versions + minor versions

            // assemble geometries

            val nodeGeoms = ProcessOSM
              // TODO add snapshot arg
              .constructPointGeometries(referencedNodes)
              .withColumn("minorVersion", lit(0))
              .withColumn("geometryChanged", lit(true))

            val now = Timestamp.from(Instant.now)

            // TODO only snapshot if source data is a snapshot
            val wayGeoms = ProcessOSM.reconstructWayGeometries(
              referencedWays,
              referencedWayNodes,
              _nodesToWays = Some(nodesToWays.withColumn("validUntil", lit(null))),
              snapshot = Some(now))

            // TODO union w/ multipolygons to support recursive relations
            val geoms = nodeGeoms.union(wayGeoms)

            // TODO support recursive relations, e.g. 3778631 (UN Plaza w/ a multipolygon relation as the outline)
            val recursiveRelations =
              relations.where(containsMemberType(lit(ProcessOSM.RelationType), 'members))

//            val multiPolygons: Dataset[Row] = ???

            // TODO only snapshot if source data is a snapshot (be nice if we could designate that as a trait on DataFrame)
            val relationGeoms = ProcessOSM.reconstructBuildingRelationGeometries(
              // relations,
              // Map types can't exist on DataFrames being intersected so we intersect on just the IDs
              relations.select('id).except(recursiveRelations.select('id)).join(relations.select('id, 'tags), Seq("id")),
              // geoms.union(multiPolygons),
              geoms,
              snapshot = Some(now)
            )

            relationGeoms
              .where('geom.isNotNull)
              // TODO add an option for GeoJSON output (vs. ORC)
              .withColumn("wkt", ST_AsText('geom))
              .drop('geom)
              .orderBy('id, 'version, 'updated)
              .repartition(1)
              .write
              .mode(SaveMode.Overwrite)
              .orc(outGeoms)

            ss.stop()

            println("Done.")
        }
      }
    )
