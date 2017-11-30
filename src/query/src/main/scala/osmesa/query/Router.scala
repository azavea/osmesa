package osmesa.query

import osmesa.query.model._

import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.HttpMethods._
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import ch.megard.akka.http.cors.scaladsl.settings._
import cats._
import cats.implicits._
import io.swagger.annotations._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object Router {
  val settings =
    CorsSettings.defaultSettings.copy(
      allowedMethods = scala.collection.immutable.Seq(GET, POST, PUT, HEAD, OPTIONS, DELETE)
    )

  def routes =
    cors(settings) {
      pathEndOrSingleSlash {
        complete {
          List(
            "/hashtags/",
            "/hashtags/{hashtag}/",
            "/hashtags/{hashtag}/users",
            "/hashtags/{hashtag}/users/{user}",
            "/users/",
            "/users/user/"
          ).mkString("<br />")
        }
      } ~
      pathPrefix("hashtags") {
        pathEndOrSingleSlash {
          complete {
            val hashtags: Option[List[Hashtag]] = (1 to scala.util.Random.nextInt(5) + 2).map({ i => Hashtag.random }).toList.sequence
            hashtags
          }
        } ~
        pathPrefix(Segment) { hashtag =>
          pathEndOrSingleSlash {
            complete {
              Hashtag.random
            }
          } ~
          pathPrefix("users") {
            pathEndOrSingleSlash {
              complete {
                val users: Option[List[User]] = (1 to scala.util.Random.nextInt(3) + 2).map({ i => User.random }).toList.sequence
                users
              }
            } ~
            pathPrefix(Segment) { uid =>
              pathEndOrSingleSlash {
                complete {
                  User.random
                }
              }
            }
          }
        }
      } ~
      pathPrefix("users") {
        pathEndOrSingleSlash {
          complete {
            val users: Option[List[LightUser]] = (1 to scala.util.Random.nextInt(5) + 2).map({ i => LightUser.random }).toList.sequence
            users
          }
        } ~
        pathPrefix(Segment) { uid =>
          complete {
            LightUser.random
          }
        }
      }
    }
}

