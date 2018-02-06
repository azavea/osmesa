package osmesa.common.model

import osmesa.common.util.Corpus

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.{Decoder, Encoder}

import scala.util.Random


case class CampaignParticipation(
  uid: Long,
  name: String,
  edits: Int
)

object CampaignParticipation {
  implicit val customConfig: Configuration = Configuration.default.withSnakeCaseMemberNames.withDefaults
  implicit val encoder: Encoder[CampaignParticipation] = deriveEncoder
  implicit val decoder: Decoder[CampaignParticipation] = deriveDecoder

  def random =
    CampaignParticipation(
      math.abs(Random.nextLong),
      Corpus.randomName,
      Random.nextInt(100)
    )
}

