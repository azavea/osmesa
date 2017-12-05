package osmesa.query.util

import osmesa.common._
import osmesa.query.model._

import geotrellis.util.LazyLogging
import io.circe._
import io.circe.syntax._
import cats.implicits._
import com.monovore.decline._
import osmesa.query.relational.tables._
import osmesa.query.util._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, BufferedMutator}
import org.apache.hadoop.hbase.util._
import org.apache.spark.sql._
import awscala._
import s3._
import com.amazonaws.services.s3.model.ObjectMetadata

import java.nio.charset._


object MockIngest extends CommandApp(
  name = "Ingest random statistics values to HBase for testing purposes",
  header = "Generate test data for OSMesa",
  main = {
    val userO = Opts.option[Int]("users", short = "u", metavar = "count", help = "Set a number of (generated) users to ingest.")
    val campaignO = Opts.option[Int]("campaign", short = "c", metavar = "count", help = "Set a number of (generated) campaigns to ingest.")
    val bucketO = Opts.option[String]("bucket", short = "b", metavar = "bucket", help = "Where should this data be stored on S3?")
    val prefixO = Opts.option[String]("prefix", short = "p", metavar = "prefix", help = "Where should data be stored within this bucket?")

    (userO, campaignO, bucketO, prefixO).mapN({ (userCount, campaignCount, bucketString, prefix) =>
      val s3 = S3.at(Region.US_EAST_1)
      val bucket = Bucket(bucketString)

      def storeUser(key: String, value: Json): Unit =
        s3.put(bucket, s"${prefix}/users/${key}.json", value.noSpaces.getBytes(StandardCharsets.UTF_8.name()), new ObjectMetadata())

      def storeCampaign(key: String, value: Json): Unit =
        s3.put(bucket, s"${prefix}/campaigns/${key}.json", value.noSpaces.getBytes(StandardCharsets.UTF_8.name()), new ObjectMetadata())

      (1 to userCount).foreach({ _ =>
        val usr = User.random
        storeUser(usr.uid.toString, usr.asJson)
      })
      (1 to campaignCount).foreach({ _ =>
        val campaign = Campaign.random
        storeCampaign(campaign.tag, campaign.asJson)
      })
    })
  }
)

