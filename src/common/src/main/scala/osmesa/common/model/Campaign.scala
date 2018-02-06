package osmesa.common.model

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.{Decoder, Encoder}

import scala.util.Random

case class Campaign(
  tag: String,
  extentUri: String,
  buildingsAdd: Int,
  buildingsMod: Int,
  roadsAdd: Int,
  kmRoadsAdd: Double,
  roadsMod: Int,
  kmRoadsMod: Double,
  waterwaysAdd: Int,
  kmWaterwaysAdd: Double,
  poiAdd: Int,
  users: List[CampaignParticipation]
)

object Campaign {
  implicit val customConfig: Configuration = Configuration.default.withSnakeCaseMemberNames.withDefaults
  implicit val encoder: Encoder[Campaign] = deriveEncoder
  implicit val decoder: Decoder[Campaign] = deriveDecoder

  def random =
    Campaign(
      HashtagCount.tags.takeRandom.getOrElse("#kony2012"),
      "https://s3.amazonaws.com/vectortiles/test-vts/peruser-2/piaco_dk/{z}/{x}/{y}.mvt",
      Random.nextInt(10000),
      Random.nextInt(10000),
      Random.nextInt(10000),
      Random.nextDouble * 10000,
      Random.nextInt(10000),
      Random.nextDouble * 10000,
      Random.nextInt(10000),
      Random.nextDouble * 10000,
      Random.nextInt(10000),
      (1 to Random.nextInt(5) + 2).map({ _ => CampaignParticipation.random }).toList
    )
}

