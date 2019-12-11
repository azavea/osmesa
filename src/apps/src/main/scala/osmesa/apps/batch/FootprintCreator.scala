package osmesa.apps.batch

import java.net.URI
import java.nio.file.{Files, Paths}

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.locationtech.geomesa.spark.jts._
import osmesa.analytics.{Analytics, Footprints, S3Utils}
import vectorpipe.functions._
import vectorpipe.functions.osm._

import scala.collection.JavaConversions._

object FootprintCreator
    extends CommandApp(
      name = "footprint-creator",
      header = "Create footprint vector tiles",
      main = {

        val historyOpt = Opts
          .option[URI]("history", help = "URI of the history ORC file to process.")
        val hashtagsOpt =
          Opts.option[URI]("include-hashtags", help = "URI containing hashtags to consider.").orNone
        val outputOpt = Opts.option[URI]("out", help = "Base URI for output.")
        val typeOpt =
          Opts
            .option[String]("type", "Type of footprints to generate (users, hashtags)")
            .withDefault("users")
            .validate("Type must be users or hashtags") {
              Set("users", "hashtags").contains
            }
        val changesetsOpt = Opts
          .option[URI]("changesets", help = "URI of the changesets ORC file to process.")
          .orNone

        (
          historyOpt,
          changesetsOpt,
          hashtagsOpt,
          outputOpt,
          typeOpt
        ).mapN { (historyURI, changesetsURI, hashtagsURI, outputURI, footprintType) =>
          Footprint.run(historyURI, changesetsURI, hashtagsURI, outputURI, footprintType)
        }
      }
    )

object Footprint extends Logging {
  def run(historyURI: URI,
          changesetsURI: Option[URI],
          hashtagsURI: Option[URI],
          outputURI: URI,
          footprintType: String): Unit = {
    implicit val spark: SparkSession = Analytics.sparkSession("Footprint")
    import spark.implicits._

    val targetHashtags = hashtagsURI match {
      case Some(uri) =>
        val lines: Seq[String] = uri.getScheme match {
          case "s3" =>
            S3Utils.readText(uri.toString).split("\n")
          case "file" =>
            Files.readAllLines(Paths.get(uri))
          case _ => throw new NotImplementedError(s"${uri.getScheme} scheme is not implemented.")
        }

        lines.filter(_.nonEmpty).map(_.trim).map(_.toLowerCase).toSet
      case None => Set.empty[String]
    }

    val history = footprintType match {
      case "users" =>
        if (targetHashtags.isEmpty) {
//          throw new RuntimeException("Refusing to generate footprints for all users")
          spark.read
            .orc(historyURI.toString)
            // TODO this is dataset-specific
            .where(!('uid isin (0, 1)))
            // use the user id as the footprint key
            .withColumnRenamed("uid", "key")
        } else {
          logInfo(s"Finding users who've participated in ${targetHashtags.mkString(", ")}")

          // for hashtag access
          val changesets =
            spark.read
              .orc(changesetsURI.getOrElse {
                throw new RuntimeException(
                  "Changesets are required when generating hashtag footprints")
              }.toString)

          val targetUsers = changesets
            .withColumn("hashtag",
                        explode(
                          merge_sets(hashtags('tags.getField("comment")),
                                     hashtags('tags.getField("hashtags")))))
            .where('hashtag isin (targetHashtags.toSeq: _*))
            .select('uid)
            .distinct

          spark.read
            .orc(historyURI.toString)
            .join(targetUsers, Seq("uid"))
            // use the user id as the footprint key
            .withColumnRenamed("uid", "key")
        }
      case "hashtags" =>
        if (targetHashtags.isEmpty) {
//          throw new RuntimeException("Refusing to generate footprints for all hashtags")
          logInfo(s"Finding changesets containing hashtags")
          val changesets =
            spark.read
              .orc(changesetsURI.toString)
              .where(size(merge_sets(hashtags('tags.getField("comment")),
                                     hashtags('tags.getField("hashtags"))) > 0))
              .withColumn("hashtag",
                          explode(
                            merge_sets(hashtags('tags.getField("comment")),
                                       hashtags('tags.getField("hashtags")))))
              .withColumnRenamed("id", "changeset")

          spark.read
            .orc(historyURI.toString)
            .join(changesets, Seq("changeset"))
            // use the hashtag as the footprint key
            .withColumnRenamed("hashtag", "key")
        } else {
          logInfo(s"Finding changesets containing these hashtags: ${targetHashtags.mkString(", ")}")
          val changesets =
            spark.read
              .orc(changesetsURI.toString)
              .withColumnRenamed("id", "changeset")

          val targetChangesets = changesets
            .withColumn("hashtag",
                        explode(
                          merge_sets(hashtags('tags.getField("comment")),
                                     hashtags('tags.getField("hashtags")))))
            .where('hashtag isin (targetHashtags.toSeq: _*))
            .select('changeset, 'hashtag)
            .distinct

          spark.read
            .orc(historyURI.toString)
            .join(targetChangesets, Seq("changeset"))
            // use the hashtag as the footprint key
            .withColumnRenamed("hashtag", "key")
        }
      case _ => throw new RuntimeException("Unrecognized footprint type")
    }

    // TODO accept a list of users

    val nodes = history
      .where('type === "node" and 'lat.isNotNull and 'lon.isNotNull)
      .withColumn("lat", asDouble('lat))
      .withColumn("lon", asDouble('lon))
      .select(st_makePoint('lon, 'lat) as 'geom, 'key)

    val stats = Footprints.create(nodes, outputURI)

    println(s"${stats.count} tiles created.")

    spark.stop()
  }
}
