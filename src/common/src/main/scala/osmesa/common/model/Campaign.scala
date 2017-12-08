package osmesa.common.model

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.{Decoder, Encoder}

import scala.util.Random


case class Campaign(
  tag: String,
  roadCountAdd: Int,
  roadsCountMod: Int,
  buildingCountAdd: Int,
  buildingCountMod: Int,
  waterwayCountAdd: Int,
  poiCountAdd: Int,
  roadKmAdd: Double,
  roadKmMod: Double,
  waterwayKmAdd: Double
)

object Campaign {
  implicit val customConfig: Configuration = Configuration.default.withSnakeCaseKeys.withDefaults
  implicit val encoder: Encoder[Campaign] = deriveEncoder
  implicit val decoder: Decoder[Campaign] = deriveDecoder

  def random = Hashtag.tags.takeRandom.map({ randomTag =>
    Campaign(
      randomTag,
      Random.nextInt(10000),
      Random.nextInt(10000),
      Random.nextInt(10000),
      Random.nextInt(10000),
      Random.nextInt(10000),
      Random.nextInt(10000),
      Random.nextDouble * 10000,
      Random.nextDouble * 10000,
      Random.nextDouble * 10000
    )
  })
}

