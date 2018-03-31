package osmesa

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import osmesa.functions.osm._
import osmesa.ingest.util.Caching


/*
 * Usage example:
 *
 * sbt "project ingest" assembly
 *
 * spark-submit \
 *   --class osmesa.DumpRelationMembers \
 *   ingest/target/scala-2.11/osmesa-ingest.jar \
 *   --orc $HOME/data/rhode-island.orc \
 *   --cache $HOME/.orc \
 *   --out $HOME/data/members \
 *   --id 148838 \
 *   --version 187 \
 */

object DumpRelationMembers extends CommandApp(
  name = "dump-relation-members",
  header = "Dump relation members",
  main = {

    /* CLI option handling */
    val orcO = Opts.option[String]("orc", help = "Location of the .orc file to process")
    val outMembersO = Opts.option[String]("out", help = "Location of the ORC file to write containing members")
    val relationIdO = Opts.option[Long]("id", help = "Relation ID")
    val relationVersionO = Opts.option[Long]("version", help = "Relation version")
    val cacheDirO = Opts.option[String]("cache", help = "Location to cache ORC files").withDefault("")

    (orcO, outMembersO, relationIdO, relationVersionO, cacheDirO).mapN { (orc, outMembers, relationId, relationVersion, cacheDir) =>
      /* Settings compatible with both local and EMR execution */
      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("extract-multipolygons")
        .set("spark.serializer", classOf[org.apache.spark.serializer.KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[geotrellis.spark.io.kryo.KryoRegistrator].getName)
        .set("spark.sql.orc.impl", "native")

      implicit val ss: SparkSession = SparkSession.builder
        .config(conf)
        .enableHiveSupport
        .getOrCreate

      import ss.implicits._

      // quiet Spark
      Logger.getRootLogger.setLevel(Level.WARN)

      val df = ss.read.orc(orc)

      val cache = Option(new URI(cacheDir).getScheme) match {
        case Some("s3") => Caching.onS3(cacheDir)
        // bare paths don't get a scheme
        case None if cacheDir != "" => Caching.onFs(cacheDir)
        case _ => Caching.none
      }

      // DOWN: get all versions of raw elements

      // get all ways referenced by relations
      val relations = ProcessOSM.preprocessRelations(df.where('type === "relation"))

      val wayIds = relations
        .where(isMultiPolygon('tags))
        .select(explode('members).as('member))
        .where($"member.type" === "way")
        .select($"member.ref".as("id"))
        .distinct

      // get all nodes referenced by referenced ways
      val ways = df.where('type === "way")

      val referencedWays = ways
        .join(wayIds, Seq("id"))

      // create a lookup table for node → ways (using only the ways we'd previously identified)
      val nodesToWays = ProcessOSM.preprocessWays(referencedWays)
        .select(explode('nds).as('id), 'id.as('way_id), 'version, 'timestamp, 'validUntil)

      // extract the referenced nodes from the lookup table
      val nodeIds = nodesToWays
        .select('id)
        .distinct

      val nodes = df.where('type === "node")

      val referencedNodes = ProcessOSM.preprocessNodes(nodes)
        .join(nodeIds, Seq("id"))

      // assemble way geometries

      val wayGeoms = cache.orc("way-geoms") {
        ProcessOSM.reconstructWayGeometries(referencedWays, referencedNodes, Some(nodesToWays))
      }

      val members = relations
        .where(isMultiPolygon('tags) and 'id === relationId and 'version === relationVersion)
        .select('changeset, 'id, 'version, 'timestamp, posexplode_outer('members).as(Seq("idx", "member")))
        .select(
          'changeset,
          'id,
          'version,
          'timestamp,
          'idx,
          'member.getField("type").as("type"),
          'member.getField("ref").as("ref"),
          'member.getField("role").as("role")
        )
        .join(wayGeoms.select('id.as("ref"), 'updated, 'validUntil, 'geom), Seq("ref"), "left_outer")
        .where(wayGeoms("updated") <= relations("timestamp") and relations("timestamp") < coalesce(wayGeoms("validUntil"), current_timestamp))

      members
        .repartition(1).write.orc(outMembers)

      ss.stop()

      println("Done.")
    }
  }
)