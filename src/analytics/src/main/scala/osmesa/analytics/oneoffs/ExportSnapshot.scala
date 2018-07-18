package osmesa.analytics.oneoffs

import osmesa.analytics._

import cats.implicits._
import com.monovore.decline._
import geotrellis.spark.io.s3.S3Client
import geotrellis.spark.util.KryoWrapper
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import spray.json._

import java.nio.charset.Charset
import java.sql.Timestamp
import java.time._
import java.time.format._
import scala.io.Source
import scala.util._

object ExportSnapshotCommand extends CommandApp(

  name   = "export-snapshot",
  header = "Exports a snapshot of OSM based on history and changeset ORCs, a bounding box, and a point in time.",
  main   = {

    val historyO = Opts.option[String]("history", help = "URI to the history ORC file to process.")
    val boundaryO = Opts.option[String]("boundary", help = "URI to geojson of boundary")
    val timeO = Opts.option[String]("time", help = "ISO 8601 format of snapshot date.")
    val snapshotPathO = Opts.option[String]("snapshotPath", help = "URI for output snapshot ORC")
    val includeHistoryO = Opts.flag("history", help = "Include historical elements").orFalse

    (
      historyO,
      boundaryO,
      timeO,
      snapshotPathO,
      includeHistoryO
    ).mapN { (historyUri, boundaryUri, timeStr, snapshotUri, includeHistory) =>
      Try(LocalDateTime.parse(timeStr)) match {
        case Success(time) =>
          ExportSnapshot.run(historyUri, boundaryUri, time, snapshotUri, includeHistory)
        case Failure(e) =>
          e match {
            case _: DateTimeParseException =>
              println(s"Could not parse date string '${timeStr}'. Make sure it's in ISO 8601 format. Parse error: $e")
              sys.exit(1)
            case _ =>
              throw e
          }
      }
    }
  }
)

object ExportSnapshot {
  def readFile(path: String): String = {
    val uri = new java.net.URI(path)
    uri.getScheme.toLowerCase match {
      case "s3" =>
        val bucket = uri.getHost
        val key = uri.getPath.drop(1)

        val is = S3Client.DEFAULT.getObject(bucket, key).getObjectContent
        try {
          Source.fromInputStream(is)(Charset.forName("UTF-8")).mkString
        } finally {
          is.close()
        }
      case _ =>
        val src = Source.fromFile(path)
        try {
          src.mkString
        } finally {
          src.close
        }
    }
  }

  def run(
    historyUri: String,
    boundaryUri: String,
    time: LocalDateTime,
    snapshotUri: String,
    includeHistory: Boolean
  ): Unit = {

    val mp = {
      // Allow the GeoJSON to be a Polygon, MultiPolygon, or FeatureCollection with Polygons or MultiPolygons
      val polygons =
        Try(readFile(boundaryUri).parseGeoJson[Polygon]).map(List(_)).getOrElse(List[Polygon]()) :::
        Try(readFile(boundaryUri).parseGeoJson[MultiPolygon]).map(_.polygons.toList).getOrElse(List[Polygon]()) :::
        Try(readFile(boundaryUri).parseGeoJson[JsonFeatureCollection].getAll[MultiPolygon]).map { collection =>
          collection.getAll[Polygon].toList :::
          collection.getAll[MultiPolygon].flatMap(_.polygons).toList
        }.getOrElse(List[Polygon]())

      MultiPolygon(polygons)
    }

    implicit val spark = Analytics.sparkSession("StatsJob")

    try {
      val history = spark.read.orc(historyUri)
      val df = createSnapshotDataFrame(history, mp, time, includeHistory)
      df.write.format("orc").mode("overwrite").save(snapshotUri)
    } finally {
      spark.stop()
    }
  }

  def createSnapshotDataFrame(
    history: DataFrame,
    boundary: MultiPolygon,
    time: LocalDateTime,
    includeHistory: Boolean
  )(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val preparedGeom = KryoWrapper(boundary.prepare)
    val boundingBox = boundary.envelope
    val isInBoundary =
      udf { (lat: Double, lon: Double) =>
        if(boundingBox.contains(lon, lat)) {
          preparedGeom.value.contains(Point(lon, lat))
        } else { false }
      }

    val idByUpdated =  Window.partitionBy('id).orderBy('version)

    val ts = Timestamp.valueOf(time)
    val timeFiltered =
      history.
        withColumn(
          "snapshotVisible",
          when((lead('version, 1) over idByUpdated).isNotNull,
            "true"
          ).otherwise('visible)
        ).
        where($"timestamp"  <= ts)

    val nodeIds =
      timeFiltered.
        where('type === "node").
        where(isInBoundary('lat, 'lon)).
        select('id)

    val wayIds =
      timeFiltered.
        where('type === "way").
        select('id, explode($"nds").as("nodeId")).
        join(nodeIds.select('id.as("nodeId")), "nodeId").
        drop('nodeId).
        distinct

    val nodeIdsFromWayIds =
      timeFiltered.
        where('type === "way").
        join(wayIds, "id").
        select(explode($"nds").as("id")).
        distinct

    // We want to create a DF that has the relation ID,
    // the type of related element that is a way or node,
    // and the target ID.
    // OSM can have relations of relations, so we need to flatten any
    // relations out in a loop.

    val relationsToTargets =
      timeFiltered.
        where('type === "relation").
        select('id, explode('members).as("member")).
        select('id, $"member.type".as("targetType"), $"member.id".as("targetId"))

    var flattenedRelationTargets =
      relationsToTargets.
        where('targetType =!= "relation")

    var relationsOfRelations =
      relationsToTargets.
        where('targetType === "relation").
        select('id, 'targetId.as("relationId"))

    while(relationsOfRelations.count() > 0) {
      val df =
        relationsToTargets.select('id.as("joinId"), 'targetId, 'targetType)

      val flattened =
        relationsOfRelations.
          join(relationsToTargets, relationsOfRelations.col("relation") === df.col("joinId")).
          drop("relationId")

      flattenedRelationTargets =
        flattened.where('targetType =!= "relation").union(flattenedRelationTargets)

      relationsOfRelations =
        flattened.
          where('targetType === "relation").
          select('id, 'targetId.as("relationId"))
    }

    val nodeBasedRelationIds =
      flattenedRelationTargets.
        where('targetType === "node").
        join(nodeIds.select('id).as("targetId"), "targetId").
        select('id).
        distinct

    val wayBasedRelationIds =
      flattenedRelationTargets.
        where('targetType === "way").
        join(wayIds.select('id).as("targetId"), "targetId").
        select('id).
        distinct

    val relationIds =
      nodeBasedRelationIds.
        union(wayBasedRelationIds).
        distinct

    val membersFromRelations =
      timeFiltered.
        where('type === "relation").
        join(relationIds, "id").
        select('id, explode('members).as("member"))

    val wayIdsFromRelationIds =
      membersFromRelations.
        where($"member.type" === "way").
        select($"member.id".as("id")).
        distinct

    val nodeIdsFromWayIdsFromRelationIds = // I know, I know
      timeFiltered.
        where('type === "way").
        join(wayIdsFromRelationIds, "id").
        select(explode($"nds").as("id")).
        distinct

    val nodeIdsFromRelationIds =
      membersFromRelations.
        where($"member.type" === "node").
        select($"member.id".as("id")).
        distinct

    val desiredElements =
      Seq(
        Seq(nodeIds, nodeIdsFromWayIds, nodeIdsFromRelationIds, nodeIdsFromWayIdsFromRelationIds).
          map(_.withColumn("type", lit("node"))),
        Seq(wayIds, wayIdsFromRelationIds).
          map(_.withColumn("type", lit("way"))),
        Seq(relationIds).
          map(_.withColumn("type", lit("relation")))
      ).
        flatten.
        reduce(_ union _).
        distinct

    val snapshotDf =
      timeFiltered.
        join(
          desiredElements,
          timeFiltered.col("id") === desiredElements.col("idd") &&
            timeFiltered.col("type") === desiredElements.col("type")
        ).
        drop("visible").
        withColumnRenamed("snapshotVisible", "visible")

    if(includeHistory) { snapshotDf }
    else {
      snapshotDf.
        where('visible === true)
    }
  }
}
