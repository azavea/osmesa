package osmesa.stats

import osmesa.common.model._

import geotrellis.spark.SpatialKey
import geotrellis.vectortile.VectorTile
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import io.circe._
import io.circe.syntax._
import io.circe.parser._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.HttpMethods._
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import ch.megard.akka.http.cors.scaladsl.settings._
import com.amazonaws.services.s3.model.{ ListObjectsV2Request, ObjectListing }
import com.amazonaws.services.s3._
import awscala._
import s3._
import cats._
import cats.syntax._
import cats.implicits._
import org.apache.commons.io.IOUtils

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global


object Router {
  val awsS3 = AmazonS3ClientBuilder.defaultClient()

  val s3 = S3.at(Region.US_EAST_1)

  val settings =
    CorsSettings.defaultSettings.copy(
      allowGenericHttpRequests = true,
      allowedMethods = scala.collection.immutable.Seq(GET, POST, PUT, HEAD, OPTIONS, DELETE)
    )

  def emptyVT(z: Int, x: Int, y: Int) = {
    val extent = TileLayouts(z).mapTransform(SpatialKey(x, y))
    VectorTile(Map(), extent)
  }

  def routes(bucket: String, prefix: String) =
    cors(settings) {
      pathEndOrSingleSlash {
        complete {
          val endpoints = List(
            "/hashtags/",
            "/hashtags/{hashtag}/",
            "/hashtags/{hashtag}/users",
            "/users/",
            "/users/user/",
            "/extent/user/",
            "/extent/hashtag/"
          ).mkString("\n")
          HttpEntity(ContentTypes.`text/plain(UTF-8)`, endpoints)
        }
      } ~
      pathPrefix("hashtags") {
        parameter("start" ? "", "maxKeys".as[Int] ?) { (start, maybeMaxKeys) =>
          pathEndOrSingleSlash {
            complete {
              val maxKeys = maybeMaxKeys match {
                case Some(max) if max < 10 => max
                case _ => 10
              }
              val request = new ListObjectsV2Request()
                .withBucketName(bucket)
                .withPrefix(s"${prefix}/hashtag/")
                .withStartAfter(start)
                .withDelimiter("/")
                .withMaxKeys(maxKeys)

              val res = awsS3.listObjectsV2(request)
              val nextStartAfter = res.getStartAfter()
              val summaries = res.getObjectSummaries().asScala
              val json = summaries.map({ summary => awsS3.getObjectAsString(bucket, summary.getKey()) })
              val decodedResults = json.map({ js =>
                decode[Campaign](js).toOption
              }).toList.sequence.getOrElse(List[Campaign]())

              ResultPage[Campaign, String, Int](
                decodedResults,
                summaries.last.getKey(),
                maxKeys
              )
            }
          }
        } ~
        pathPrefix(Segment) { tag =>
          pathEndOrSingleSlash {
            complete {
              s3.get(Bucket(bucket), s"${prefix}/hashtag/${tag}.json").flatMap({ s3obj =>
                val content = scala.io.Source.fromInputStream(s3obj.content).mkString
                parse(content).toOption
              })
            }
          } ~
          pathPrefix("users") {
            pathEndOrSingleSlash {
              complete {
                val contributors: Option[List[CampaignParticipation]] =
                  s3.get(Bucket(bucket), s"${prefix}/hashtag/${tag}.json").flatMap({ s3obj =>
                    val content = scala.io.Source.fromInputStream(s3obj.content).mkString
                    val decoded: Either[Error, Campaign] = decode[Campaign](content)
                    decoded.map({ _.users.toList }).toOption
                  })
                contributors
              }
            }
          }
        }
      } ~
      pathPrefix("users") {
        parameter("start" ? "", "maxKeys".as[Int] ?) { (start, maybeMaxKeys) =>
          pathEndOrSingleSlash {
            complete {
              val maxKeys = maybeMaxKeys match {
                case Some(max) if max <= 10 => max
                case _ => 10
              }
              val request = new ListObjectsV2Request()
                .withBucketName(bucket)
                .withPrefix(s"${prefix}/user/")
                .withStartAfter(start)
                .withDelimiter("/")
                .withMaxKeys(maxKeys)

              val res = awsS3.listObjectsV2(request)
              val nextStartAfter = res.getStartAfter()
              val summaries = res.getObjectSummaries().asScala
              val json = summaries.map({ summary => awsS3.getObjectAsString(bucket, summary.getKey()) })
              val decodedResults = json.map({ js =>
                decode[User](js).toOption
              }).toList.sequence.getOrElse(List[User]())

              ResultPage[User, String, Int](
                decodedResults,
                summaries.last.getKey(),
                maxKeys
              )
            }
          }
        } ~
        pathPrefix(Segment) { uid =>
          complete {
            s3.get(Bucket(bucket), s"${prefix}/user/${uid}.json").flatMap({ s3obj =>
              val content = scala.io.Source.fromInputStream(s3obj.content).mkString
              parse(content).toOption
            })
          }
        }
      }
    } ~
    pathPrefix("extents") {
      respondWithHeaders(
        `Access-Control-Allow-Origin`(HttpOriginRange.*),
        `Access-Control-Allow-Credentials`(true),
        `Access-Control-Allow-Headers`("Authorization", "Content-Type", "X-Requested-With"),
        `Access-Control-Allow-Methods`(OPTIONS, POST, PUT, GET, DELETE)
      ) {
        pathPrefix("user") {
          pathPrefix(Segment / IntNumber / IntNumber / IntNumber) { (uid, zoom, x, y) =>
            complete {
              s3.get(Bucket(bucket), s"${prefix}/user/${uid}/${zoom}/${x}/${y}.mvt") match {
                case Some(s3obj) => vtResponse(IOUtils.toByteArray(s3obj.content))
                case None => vtResponse(emptyVT(zoom, x, y).toBytes)
              }
            }
          }
        } ~
        pathPrefix("hashtag") {
          pathPrefix(Segment / IntNumber / IntNumber / IntNumber) { (hashtag, zoom, x, y) =>
            complete {
              s3.get(Bucket(bucket), s"${prefix}/hashtag/${hashtag}/${zoom}/${x}/${y}.mvt") match {
                case Some(s3obj) => vtResponse(IOUtils.toByteArray(s3obj.content))
                case None => vtResponse(emptyVT(zoom, x, y).toBytes)
              }
            }
          }
        }
      }
    }

  def vtResponse(bytes: Array[Byte]) =
    HttpResponse(
      entity = HttpEntity(
        new ContentType.Binary(
          MediaType.customBinary("application", "vnd.mapbox-vector-tile", new MediaType.Compressibility(false))
        ),
        bytes
      )
    )
}

