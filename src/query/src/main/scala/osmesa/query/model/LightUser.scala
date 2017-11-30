package osmesa.query.model

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.{Decoder, Encoder}
import cats._
import cats.implicits._

import scala.util.Random
import java.time.Instant


case class LightUser(
  uid: Long,
  name: String,
  roads: Int,
  buildings: Int,
  changesets: Int
)

object LightUser {
  implicit val customConfig: Configuration = Configuration.default.withSnakeCaseKeys.withDefaults
  implicit val encoder: Encoder[LightUser] = deriveEncoder
  implicit val decoder: Decoder[LightUser] = deriveDecoder

  val names = List("fred", "salvadore", "orlando", "victor", "alphonse", "giuseppe")

  def random = names.takeRandom.map({ randomName =>
    LightUser(
      Random.nextInt(10000).toLong + 1,
      randomName,
      Random.nextInt(10000),
      Random.nextInt(10000),
      Random.nextInt(10000)
    )
  })
}

