package osmesa.stats

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.http.scaladsl.server.Directives._
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import awscala._
import s3._

import scala.util.{Try, Properties}


object AkkaSystem {
  implicit val system = ActorSystem("osmesa-system")
  implicit val materializer = ActorMaterializer()
}

object Main extends App {

  implicit val system = AkkaSystem.system
  implicit val materializer = AkkaSystem.materializer

  val host: String =
    Properties.envOrElse("HOST", "0.0.0.0")

  val port: Int =
    Properties.envOrNone("PORT").map(_.toInt).getOrElse(80)

  val bucket =
    Properties.envOrElse("S3_BUCKET", "geotrellis-test")

  val prefix =
    Properties.envOrElse("S3_PREFIX", "nathan/osm-stats")

  sys.addShutdownHook {
    Try(system.terminate())
  }

  Http().bindAndHandle(Router.routes(bucket, prefix), host, port)
}

