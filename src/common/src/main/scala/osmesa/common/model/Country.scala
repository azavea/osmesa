package osmesa.common.model

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.{Decoder, Encoder}

import scala.util.Random


case class Country(
  name: String,
  count: Int
)

object Country {
  implicit val customConfig: Configuration = Configuration.default.withSnakeCaseKeys.withDefaults
  implicit val encoder: Encoder[Country] = deriveEncoder
  implicit val decoder: Decoder[Country] = deriveDecoder

  val countries = List("indonesia", "south africa", "colombia", "greece", "iraq", "iran", "cambodia", "cuba", "USA", "germany", "france", "russia")

  def random = countries.takeRandom.map({ randomCountry =>
    Country(
      randomCountry,
      Random.nextInt(10000)
    )
  })

}

