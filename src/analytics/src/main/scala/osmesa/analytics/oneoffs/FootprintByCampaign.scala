package osmesa.analytics.oneoffs

import osmesa.analytics._

import com.amazonaws.services.s3.model.CannedAccessControlList._
import com.monovore.decline._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.io.s3._
import geotrellis.util._
import geotrellis.vector._
import geotrellis.vectortile._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import vectorpipe._

object FootprintByCampaign {
  // These hashtags were a sensible group that had over 100K changesets.
  val TARGET_HASHTAGS =
    Set(
      "#missingmaps",
      "#redcross",
      "#eliminatemalaria",
      "#tanzania",
      "#mapgive",
      "#peacecorps",
      "#maproulette",
      "#tanzaniadevelopmenttrust"
    )

  def main(args: Array[String]): Unit = {
    implicit val ss = Analytics.sparkSession("Footprint By Campaign")
    import ss.implicits._

//    val outputPath = args(0)
    //    val bufferSize = args(1).toDouble
    val bufferSize = 0.001

    val HashtagSet = raw"#(\w+)".r
    val breakOutHashtags =
      udf[Seq[String], Map[String, String]] { tags =>
        var hashtags = List[String]()
        tags.get("comment") match {
          case Some(s) =>
            for (m <- HashtagSet.findAllMatchIn(s)) { hashtags = hashtags :+ m.group(0).toLowerCase }
          case None =>
        }

        hashtags
      }

    val partitioner = new HashPartitioner(20000)

    val history = OSMOrc.planetHistory
    val changesets = OSMOrc.changesets

    val historyNodes =
      history
        .select("lat", "lon", "changeset")
        .where("type = 'node'")

    val changesetToHashtag =
      changesets
        .where($"tags".getItem("comment").contains("#"))
        .withColumn("hashtags", breakOutHashtags($"tags"))
        .where(size($"hashtags") > 0)
        .select($"id", explode($"hashtags").alias("hashtag"))
        .map { row =>
          (row.getAs[Long]("id"), row.getAs[String]("hashtag"))
        }

    // Find hashtag -> changeset count
    // changesetToHashtag.groupBy("hashtag").collect()

    val filteredChangesetToHashtag =
      changesetToHashtag
        .rdd
        .filter { case (_, hashtag) =>
          TARGET_HASHTAGS.contains(hashtag)
        }
        .partitionBy(partitioner)


    val changesetToHistoryNodes =
      historyNodes
        .map { row =>
          val lat = row.getAs[java.math.BigDecimal]("lat").doubleValue()
          val lon = row.getAs[java.math.BigDecimal]("lon").doubleValue()
          val changeset = row.getAs[Long]("changeset")
          (changeset, (lon, lat))
        }
        .rdd
        .partitionBy(partitioner)

    val features: RDD[Feature[Geometry, String]] =
      filteredChangesetToHashtag
        .join(changesetToHistoryNodes)
        .map { case (_, (hashtag, (lon, lat))) =>
          (hashtag, Point(lon, lat).reproject(LatLng, WebMercator).buffer(bufferSize): Geometry)
        }
        .reduceByKey { (g1, g2) =>
          Geometry(g1.jtsGeom.union(g2.jtsGeom))
        }
        .map { case (hashtag, geom) =>
          Feature(geom, hashtag)
      }

    for(z <- 1 to 7) {

      val layoutDefinition = ZoomedLayoutScheme(WebMercator).levelForZoom(z).layout

      val gridded: RDD[(SpatialKey, Iterable[Feature[Geometry, String]])] =
        VectorPipe.toGrid(Clip.byBufferedExtent, VectorPipe.logToStdout, layoutDefinition, features)

      val vectorTiles: RDD[(SpatialKey, VectorTile)] =
        VectorPipe.toVectorTile(
          { (tileExtent: Extent, geoms: Iterable[Feature[Geometry, String]]) =>
            Collate.generically(
              tileExtent,
              geoms,
              { _: Feature[Geometry, String] => "hashtag_footprint" },
              { v: String => Map("hashtag" -> VString(v)) }
            )
          },
          layoutDefinition,
          gridded
        )

      val s3PathFromKey: SpatialKey => String =
        SaveToS3.spatialKeyToPath(
          LayerId("hashtags", z),  // Whatever zoom level it is
          "s3://vectortiles/test-vts/{name}/{z}/{x}/{y}.mvt"
        )

      vectorTiles
        .mapValues(_.toBytes)
        .saveToS3(s3PathFromKey, putObjectModifier = { o => o.withCannedAcl(PublicRead)})
    }
  }
}
