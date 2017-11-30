package osmesa.query.model

import osmesa.query.util._

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.{Decoder, Encoder}

import scala.util.Random


case class CountryCount(
  name: String,
  count: Int
)

object CountryCount {
  implicit val customConfig: Configuration = Configuration.default.withSnakeCaseMemberNames.withDefaults
  implicit val encoder: Encoder[CountryCount] = deriveEncoder
  implicit val decoder: Decoder[CountryCount] = deriveDecoder

  def random =
    CountryCount(
      Corpus.randomCountry,
      Random.nextInt(10000)
    )
}

