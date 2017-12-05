package osmesa.query.model

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.{Decoder, Encoder}

import scala.util.Random
import java.time.Instant


case class DayCount(
  day: Instant,
  count: Int
)

object DayCount {
  implicit val customConfig: Configuration = Configuration.default.withSnakeCaseMemberNames.withDefaults
  implicit val encoder: Encoder[DayCount] = deriveEncoder
  implicit val decoder: Decoder[DayCount] = deriveDecoder

  def random =
    DayCount(
      Instant.now,
      Random.nextInt(10000)
    )
}

