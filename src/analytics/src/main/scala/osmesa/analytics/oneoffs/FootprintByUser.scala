package osmesa.analytics.oneoffs

import osmesa.analytics._

import com.amazonaws.services.s3.model.CannedAccessControlList._
import com.monovore.decline._
import com.vividsolutions.jts.{geom => jts}
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

case class ChangesetFootprint(user: String, changeset: Long, numberOfNodes: Int, width: Double, height: Double)

object FootprintByUser {
  val BASE_ZOOM = 13
  val LAYER_NAME = "user_footprint"

  def save(zoom: Int, vectorTiles: RDD[((SpatialKey, String), VectorTile)]) = {
    val s3PathFromKey: ((SpatialKey, String)) => String =
      { case (sk, name) =>
        s"s3://vectortiles/test-vts/peruser/${name}/${zoom}/${sk.col}/${sk.row}.mvt"
      }
    vectorTiles
      .mapValues(_.toBytes)
      .saveToS3(s3PathFromKey, putObjectModifier = { o => o.withCannedAcl(PublicRead)})
  }

  def bakeVectorTiles(
    rdd: RDD[((SpatialKey, String), Iterable[Feature[Geometry, ChangesetFootprint]])],
    layout: LayoutDefinition
  ): RDD[((SpatialKey, String), VectorTile)] =
    rdd
      .mapPartitions({ partition =>
        val mapTransform = layout.mapTransform
        partition.map { case ((spatialKey, user), pointFeatures) =>
          val tileExtent = mapTransform(spatialKey)
          val vtFeatures =
            pointFeatures.flatMap { pf =>
              pf.geom match {
                case p: Point =>
                  val cf = pf.data
                  val vtData =
                    Map(
                      "user" -> VString(cf.user),
                      "changeset" -> VInt64(cf.changeset),
                      "numberOfNodes" -> VInt64(cf.numberOfNodes),
                      "width" -> VDouble(cf.width),
                      "height" -> VDouble(cf.width)
                    )
                  Some(Feature(p, vtData))
                case _ => None
              }
            }
          val layer =
            StrictLayer(
                name=LAYER_NAME,
                tileWidth=4096,
                version=2,
                tileExtent=tileExtent,
                points=vtFeatures.toSeq,
                multiPoints=Seq[Feature[MultiPoint, Map[String, Value]]](),
                lines=Seq[Feature[Line, Map[String, Value]]](),
                multiLines=Seq[Feature[MultiLine, Map[String, Value]]](),
                polygons=Seq[Feature[Polygon, Map[String, Value]]](),
                multiPolygons=Seq[Feature[MultiPolygon, Map[String, Value]]]()
              )

          val vt = VectorTile(Map(LAYER_NAME -> layer), tileExtent)

         ((spatialKey, user), vt)
        }
      }, preservesPartitioning = true)

  def main(args: Array[String]): Unit = {
    implicit val ss = Analytics.sparkSession("Footprint By User")
    import ss.implicits._

    //    val partitioner = new HashPartitioner(10000)
    val partitioner = new HashPartitioner(1000)

    val history = OSMOrc.planetHistory

    val historyNodes =
      history
        .select("lat", "lon", "changeset", "user")
        .where("type = 'node'")
        .where("user = 'piaco_dk'") // DEBUG
//        .repartition(1000)
        //.repartition(10000)

    val changesetFootprints =
      historyNodes
        .map { row =>
          val lat = row.getAs[java.math.BigDecimal]("lat").doubleValue()
          val lon = row.getAs[java.math.BigDecimal]("lon").doubleValue()

          val key =(row.getAs[String]("user"), row.getAs[Long]("changeset"))
          val value = (lon, lat)
          (key, value)
        }
        .rdd
        .groupByKey(partitioner)
        .map { case ((user, changeset), lls) =>
          val mp = MultiPoint(lls)
          val changePoint = mp.centroid.as[Point].get
          val numberOfNodes = mp.points.length
          val bbox = mp.envelope

          Feature(changePoint, ChangesetFootprint(user, changeset, numberOfNodes, bbox.width, bbox.height))
        }

    val baseLayout = ZoomedLayoutScheme(WebMercator).levelForZoom(BASE_ZOOM).layout

    val keyedChangesetFootprints: RDD[((SpatialKey, String), Iterable[Feature[Geometry, ChangesetFootprint]])] =
      changesetFootprints
        .clipToGrid(baseLayout)
        .map { case (spatialKey, pointFeature) => ((spatialKey, pointFeature.data.user), pointFeature) }
        .groupByKey(partitioner)

    var rdd: RDD[((SpatialKey, String), Iterable[Feature[Geometry, ChangesetFootprint]])] = keyedChangesetFootprints
    for(z <- BASE_ZOOM to 1 by -1) {
      val layout = ZoomedLayoutScheme(WebMercator).levelForZoom(z).layout
      save(z, bakeVectorTiles(rdd, layout))
      rdd =
        rdd
          .map { case ((sk, u), geoms) =>
            ((SpatialKey(sk.col/2, sk.row/2), u), geoms)
          }
          .groupByKey(partitioner)
          .mapValues(_.flatten)
    }

    // val griddedPoints =
    //   VectorPipe.toGrid(
    //     Clip.byExtent,
    //     VectorPipe.logToStdout,
    //     ZoomedLayoutScheme(WebMercator).levelForZoom(15).layout,
    //     pointsByHashtag
    //   )

    // val features =
    //   griddedPoints
    //     .flatMap { case (sk, points) => points.map { p => ((sk, p.data), p.geom) } }
    //     .groupByKey()
    //     .map { case ((_, hashtag), points) =>
    //       // Buffer points, and bring

    //       val p = MultiPolygon(points.head.jtsGeom.buffer(0))


    // for(z <- 1 to 7) {

    //   val layoutDefinition = ZoomedLayoutScheme(WebMercator).levelForZoom(z).layout

    //   val gridded: RDD[(SpatialKey, Iterable[Feature[Geometry, String]])] =
    //     VectorPipe.toGrid(Clip.byBufferedExtent, VectorPipe.logToStdout, layoutDefinition, features)

    //   val vectorTiles: RDD[(SpatialKey, VectorTile)] =
    //     VectorPipe.toVectorTile(
    //       { (tileExtent: Extent, geoms: Iterable[Feature[Geometry, String]]) =>
    //         Collate.generically(
    //           tileExtent,
    //           geoms,
    //           { _: Feature[Geometry, String] => "hashtag_footprint" },
    //           { v: String => Map("hashtag" -> VString(v)) }
    //         )
    //       },
    //       layoutDefinition,
    //       gridded
    //     )

    //   val s3PathFromKey: SpatialKey => String =
    //     SaveToS3.spatialKeyToPath(
    //       LayerId("hashtags", z),  // Whatever zoom level it is
    //       "s3://vectortiles/test-vts/{name}/{z}/{x}/{y}.mvt"
    //     )

    //   vectorTiles
    //     .mapValues(_.toBytes)
    //     .saveToS3(s3PathFromKey, putObjectModifier = { o => o.withCannedAcl(PublicRead)})
    // }
  }
}
