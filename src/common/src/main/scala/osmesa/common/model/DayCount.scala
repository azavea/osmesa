package osmesa.common.model

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.{Decoder, Encoder}
import io.circe.java8.time._

import scala.util.Random
import java.time._


case class DayCount(
  day: LocalDate,
  count: Int
)

object DayCount {
  implicit val customConfig: Configuration = Configuration.default.withSnakeCaseMemberNames.withDefaults
  implicit val encoder: Encoder[DayCount] = deriveEncoder
  implicit val decoder: Decoder[DayCount] = deriveDecoder

  def random =
    DayCount(
      new java.util.Date(scala.util.Random.nextInt(Int.MaxValue).toLong + 1199999999999L).toInstant.atOffset(ZoneOffset.UTC).toLocalDate,
      Random.nextInt(10)
    )
}
