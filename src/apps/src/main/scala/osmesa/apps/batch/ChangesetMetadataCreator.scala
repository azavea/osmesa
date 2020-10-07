import osmesa.apps.batch

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import osmesa.analytics.Analytics
import vectorpipe.model.ChangesetComment

import java.net.URI

object ChangesetMetadataCreator
  extends CommandApp(
    name = "changeset-metadata",
    header = "Changeset Metadata",
    main = {

      val changesetsCSVOpt = Opts.option[URI](
        "changesets",
        metavar = "CSV URI",
        help = "Location of CSV file giving dump of changesets table"
      )

      val commentsCSVOpt = Opts.option[URI](
        "comments",
        metavar = "CSV URI",
        help = "Location of CSV file giving dump of changeset comments table"
      )

      val tagsCSVOpt = Opts.option[URI](
        "tags",
        metavar = "CSV URI",
        help = "Location of CSV file giving dump of changeset tags table"
      )

      val usersCSVOpt = Opts.option[URI](
        "users",
        metavar = "users",
        help = "Location of CSV file giving dump of users table"
      )

      val outputOrcArg = Opts.argument[URI](
        metavar = "ORC URI"
      )

      (changesetsCSVOpt, commentsCSVOpt, tagsCSVOpt, usersCSVOpt, outputOrcArg).mapN {
        (changesetCSV, commentsCSV, tagsCSV, usersCSV, outputOrc) =>

        import ChangesetMetadataCreatorUtils.{getClass=>_, _}

        implicit val spark: SparkSession = Analytics.sparkSession("ChangesetStats")
        import spark.implicits._

        val logger = org.apache.log4j.Logger.getLogger(getClass())

        val csvOpts = Map(
          "header" -> "true",
          "inferSchema" -> "true",
          "multiline" -> "true"
        )

        val users = spark
          .read
          .format("csv")
          .options(csvOpts)
          .load(usersCSV.toString)

        val tags = spark
          .read
          .format("csv")
          .options(csvOpts)
          .load(changesetCSV.toString)
          .groupBy('changeset_id)
          .agg(
            'changeset_id,
            collect_list('k) as 'k,
            collect_list('v) as 'v
          ).as[ChangesetTagRaw]
          .map(_.toChangesetTag)

        val comments = spark
          .read
          .format("csv")
          .options(csvOpts)
          .load(commentsCSV.toString)
          .select(
            'changeset_id cast("Long") as 'changeset_id,
            'author_id cast("Int") as 'uid,
            'body,
            'created_at as 'date
          ).join(users.withColumnRenamed("id", "uid"), Seq("uid"), "left")
          .groupBy('changeset_id)
          .agg(
            'changeset_id,
            collect_list('date) as 'dates,
            collect_list('uid) as 'uids,
            collect_list('name) as 'users,
            collect_list('body) as 'bodies
          ).as[ChangesetCommentRaw].map(_.toChangesetComments)

        val changesets = spark
          .read
          .format("csv")
          .options(csvOpts)
          .load(changesetCSV.toString)
          .select(
            'id as 'changeset_id,
            'created_at as 'createdAt,
            lit(false) as 'open,
            'closed_at as 'closedAt,
            ('min_lat cast("Double")) / 1e7 as 'minLat,
            ('min_lon cast("Double")) / 1e7 as 'minLon,
            ('max_lat cast("Double")) / 1e7 as 'maxLat,
            ('max_lon cast("Double")) / 1e7 as 'maxLon,
            'num_changes as 'numChanges,
            'user_id as 'uid
          )

        val complete = changesets
          .join(users.withColumnRenamed("id", "uid"), Seq("uid"), "left")
          .withColumnRenamed("name", "user")
          .join(comments, Seq("changeset_id"), "left")
          .join(tags, Seq("changeset_id"), "left")
          .withColumnRenamed("changeset_id", "id")
          .withColumn("sequence", lit(-1))

        complete.repartition(1).write.orc(outputOrc.toString)

        spark.stop
      }
    }
  )

object ChangesetMetadataCreatorUtils {

  case class ChangesetTag(
    changeset_id: Long,
    tags: Map[String, String]
  )

  case class ChangesetTagRaw(
    changeset_id: String,
    k: Seq[String],
    v: Seq[String])
  {
    def toChangesetTag(): ChangesetTag = {
      ChangesetTag(changeset_id.toLong, k.zip(v).toMap)
    }
  }

  case class ChangesetCommentWithId(
    changeset_id: String,
    commentsCount: Int,
    comments: Seq[ChangesetComment]
  )

  case class ChangesetCommentRaw(
    changeset_id: String,
    dates: Seq[java.sql.Timestamp],
    uids: Seq[Int],
    users: Seq[String],
    bodies: Seq[String]
  ) {
    def toChangesetComments(): ChangesetCommentWithId = {
      ChangesetCommentWithId(
        changeset_id,
        dates.length,
        for ( i <- Range(0, dates.length).toSeq ) yield
          ChangesetComment(dates(i), users(i), uids(i), bodies(i))
      )
    }
  }

}
