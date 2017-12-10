package osmesa.query

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.http.scaladsl.server.Directives._

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


  sys.addShutdownHook {
    Try(system.terminate())
  }

  Http().bindAndHandle(Router.routes, host, port)
}
