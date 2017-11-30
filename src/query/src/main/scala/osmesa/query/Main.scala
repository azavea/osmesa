package osmesa.query

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.http.scaladsl.server.Directives._
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import awscala._
import s3._

import scala.util.{Try, Properties}


object AkkaSystem {
  implicit val system = ActorSystem("rf-system")
  implicit val materializer = ActorMaterializer()
}

object Main extends App {

  implicit val system = AkkaSystem.system
  implicit val materializer = AkkaSystem.materializer

  val host: String =
    Properties.envOrElse("HOST", "0.0.0.0")

  val port: Int =
    Properties.envOrNone("PORT").map(_.toInt).getOrElse(80)


  sys.addShutdownHook {
    Try(system.terminate())
  }

	val s3client = AmazonS3ClientBuilder.defaultClient()
  val s3 = S3.at(Region.US_EAST_1)
  val bucket = "geotrellis-test"
  val prefix = "nathan/osm-stats"
  Http().bindAndHandle(Router.routes(s3, s3client, bucket, prefix), host, port)
}

