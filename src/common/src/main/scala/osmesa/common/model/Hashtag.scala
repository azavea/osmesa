package osmesa.common.model

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.{Decoder, Encoder}

import scala.util.Random


case class Hashtag(
  tag: String,
  count: Int
)

object Hashtag {
  implicit val customConfig: Configuration = Configuration.default.withSnakeCaseKeys.withDefaults
  implicit val encoder: Encoder[Hashtag] = deriveEncoder
  implicit val decoder: Decoder[Hashtag] = deriveDecoder

  val tags = List("#campaign2016", "#campaign2017", "#angola", "#test")

  def random = tags.takeRandom.map({ randomTag =>
    Hashtag(
      randomTag,
      Random.nextInt(10000)
    )
  })
}

