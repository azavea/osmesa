package osmesa.analytics.oneoffs

import java.net.URI

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import com.vividsolutions.jts.geom.{Geometry, Point}
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.spark._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vector
import geotrellis.vectortile._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.locationtech.geomesa.spark.jts._
import osmesa.GenerateVT
import osmesa.analytics.Analytics
import osmesa.common.ProcessOSM
import osmesa.common.functions._
import osmesa.common.functions.osm._

object CentroidStats extends CommandApp(
  name = "centroid-stats",
  header = "centroid statistics",
  main = {
    val historyOpt =
      Opts.option[String]("history", help = "Location of the History ORC file to process.")
    val zoomOpt =
      Opts.option[Int](long = "zoom", help = "Zoom level of most resolute tile layer")
    val outputOpt =
      Opts.option[URI](long = "output", help = "Output URI prefix; trailing / must be included")

    (historyOpt, outputOpt, zoomOpt).mapN { (historySource, output, baseZoom) =>
      implicit val spark: SparkSession = Analytics.sparkSession("ChangesetStats").withJTS
      import spark.implicits._

      spark.sparkContext.setLogLevel("WARN")

      if (output.getScheme != "s3") {
        throw new IllegalArgumentException("Output URI must point to S3 bucket")
      }

      val logGridCells = 4 // Vector tiles will have 16x16 cells inside
      val cellZoom = baseZoom + logGridCells

      val history = spark.read.orc(historySource)

      // val pointGeoms = ProcessOSM.geocode(ProcessOSM.constructPointGeometries(
      //   // pre-filter to POI nodes
      //   history.where('type === "node" and isPOI('tags))
      // ).withColumn("minorVersion", lit(0)))

      val buildingGeoms = ProcessOSM.geocode(ProcessOSM.reconstructWayGeometries(
        // pre-filter to interesting ways
        history.where('type === "way" and (isBuilding('tags))),// or isRoad('tags) or isWaterway('tags) or isPOI('tags))),
        // let reconstructWayGeometries do its thing; nodes are cheap
        history.where('type === "node")
      ).drop('geometryChanged))

      @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

      val zls = ZoomedLayoutScheme(WebMercator)
      val layout = zls.levelForZoom(cellZoom).layout
      val keyIndex = {
        val maxIdx = math.pow(2, cellZoom).toInt
        val bounds= Bounds(SpatialKey(0,0), SpatialKey(maxIdx - 1, maxIdx - 1)).asInstanceOf[KeyBounds[SpatialKey]]
        ZCurveKeyIndexMethod.createIndex(bounds)
      }
      val toWM = org.apache.spark.sql.functions.udf[Geometry, Geometry]{g =>
        vector.Geometry(g).reproject(LatLng, WebMercator).jtsGeom
      }
      val indexWMPoint = org.apache.spark.sql.functions.udf[Long, Geometry]{ g =>
        val wmpt = vector.Point(g.asInstanceOf[Point])
        keyIndex.toIndex(layout.mapTransform(wmpt)).toLong
      }
      val wmPointToKey = org.apache.spark.sql.functions.udf[SpatialKey, Geometry]{ g =>
        val wmpt = vector.Point(g.asInstanceOf[Point])
        layout.mapTransform(wmpt)
      }
      val shiftRight = org.apache.spark.sql.functions.udf[Long, Long, Int]{(x, s) => x >> s}
      val reduceSpatialKey = org.apache.spark.sql.functions.udf[SpatialKey, SpatialKey, Int]{ (k, levels) =>
        SpatialKey(k.col >> levels, k.row >> levels)
      }

      val augmentedBuildings = buildingGeoms
        .where(not(isnull('geom)))
        .withColumn("area", st_area('geom))
        .withColumn("center", st_centroid(toWM('geom)))
        .withColumn("zindex", indexWMPoint('center))
        .withColumn("spatialKey", wmPointToKey('center))
        .withColumn("temporalKey", (col("updated").cast("long"))/lit(604800))

      augmentedBuildings.printSchema

      val cells = augmentedBuildings
        .groupBy('spatialKey, 'temporalKey)
        .agg(
          first('zindex).as('zindex),
          avg(st_x('center)).as('x),
          avg(st_y('center)).as('y),
          count('center).as('num)
         )
        .withColumn("center", st_makePoint('x, 'y))
        .drop("x", "y")

      cells.printSchema

      // val aggregateStats = augmentedBuildings
      //   .withColumn("buildings_added",
      //     when(isBuilding('tags) and 'version === 1 and 'minorVersion === 0, lit(1))
      //       .otherwise(lit(0)))
      //   .withColumn("buildings_modified",
      //     when(isBuilding('tags) and not('version === 1 and 'minorVersion === 0), lit(1))
      //       .otherwise(lit(0)))
      //   .groupBy('changeset)
      //   .agg(
      //     sum('buildings_added).as('buildings_added),
      //     sum('buildings_modified).as('buildings_modified),
      //     count_values(flatten(collect_list('countries))) as 'countries
      //   )

      val tileData = cells
        .groupBy(shiftRight(col("zindex"), lit(2 * logGridCells)).as('tileIndex))
        .agg(
          first(shiftRight($"spatialKey.col", lit(logGridCells))).as('col),
          first(shiftRight($"spatialKey.row", lit(logGridCells))).as('row),
          collect_list('center).as('centers),
          collect_list('num).as('buildingCounts),
          collect_list((col("temporalKey") * lit(604800)).cast("timestamp")).as('weekBeginning)
        )

      tileData.printSchema
      tileData.show

      val tileLayout = zls.levelForZoom(baseZoom).layout
      val keyedTiles = tileData
        .rdd
        .map{ row => {
          val locations = row.getAs[Seq[Point]]("centers")
          val buildingCounts = row.getAs[Seq[Long]]("buildingCounts")
          val date = row.getAs[Seq[java.sql.Timestamp]]("weekBeginning")
          val key = SpatialKey(row.getAs[Long]("col").toInt, row.getAs[Long]("row").toInt)
          val ex = tileLayout.mapTransform(key)

          val features = for (i <- Range(0, locations.length)) yield
            vector.PointFeature(
              vector.Point(locations(i)),
              Map("buildingsChanged" -> VInt64(buildingCounts(i)),
                  "weekBeginning" -> VInt64(date(i).getTime)))

          (key, VectorTile(Map("statistics" -> StrictLayer(name="statistics",
                                                           tileWidth=4096,
                                                           version=2,
                                                           tileExtent = ex,
                                                           points = features,
                                                           multiPoints = Seq(),
                                                           lines = Seq(),
                                                           multiLines = Seq(),
                                                           polygons = Seq(),
                                                           multiPolygons = Seq()
                                                          )),
                           ex)
          )
        }}

      val prefix = if (output.getPath.endsWith("/"))
                     output.getPath.drop(1).take(output.getPath.length - 2)
                   else
                     output.getPath.drop(1)
      GenerateVT.save(keyedTiles, baseZoom, output.getAuthority, prefix)

      spark.stop()
    }
  }
)
