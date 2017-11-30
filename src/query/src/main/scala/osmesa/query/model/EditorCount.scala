package osmesa.query.model

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.{Decoder, Encoder}

import scala.util.Random


case class EditorCount(
  tag: String,
  count: Int
)

object EditorCount {
  implicit val customConfig: Configuration = Configuration.default.withSnakeCaseMemberNames.withDefaults
  implicit val encoder: Encoder[EditorCount] = deriveEncoder
  implicit val decoder: Decoder[EditorCount] = deriveDecoder

  def random =
    EditorCount(
      HashtagCount.tags.takeRandom.getOrElse("#BackupCampaign"),
      Random.nextInt(10000)
    )
}

