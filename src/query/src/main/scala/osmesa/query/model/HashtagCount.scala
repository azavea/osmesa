package osmesa.query.model

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.{Decoder, Encoder}

import scala.util.Random


case class HashtagCount(
  tag: String,
  count: Int
)

object HashtagCount {
  implicit val customConfig: Configuration = Configuration.default.withSnakeCaseMemberNames.withDefaults
  implicit val encoder: Encoder[HashtagCount] = deriveEncoder
  implicit val decoder: Decoder[HashtagCount] = deriveDecoder

  val tags = List("campaign2016", "campaign2017", "angola", "test", "hotosm", "fixtheroads")

  def random =
    HashtagCount(
     tags.takeRandom.getOrElse("#FallbackCampaign2018"),
      Random.nextInt(10000)
    )
}

