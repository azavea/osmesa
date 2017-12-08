package osmesa.common.model

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.{Decoder, Encoder}
import cats._
import cats.implicits._

import scala.util.Random
import java.time.Instant

case class Day(instant: Instant, count: Int)

object Day {
  implicit val customConfig: Configuration = Configuration.default.withSnakeCaseKeys.withDefaults
  implicit val encoder: Encoder[Day] = deriveEncoder
  implicit val decoder: Decoder[Day] = deriveDecoder
}

case class User(
  uid: Long,
  name: String,
  geoExtent: String,
  buildingCountAdd: Int,
  buildingCountMod: Int,
  poiCountAdd: Int,
  waterwayKmAdd: Double,
  waterwayCountAdd: Int,
  roadKmAdd: Double,
  roadKmMod: Double,
  roadCountAdd: Int,
  roadCountMod: Int,
  changesetCount: Int,
  editCount: Int,
  editTimes: List[Day],
  countryList: List[Country],
  hashtags: List[Hashtag]
)

object User {
  implicit val customConfig: Configuration = Configuration.default.withSnakeCaseKeys.withDefaults
  implicit val encoder: Encoder[User] = deriveEncoder
  implicit val decoder: Decoder[User] = deriveDecoder

  val names = List("fred", "salvadore", "orlando", "victor", "alphonse", "giuseppe")

  def random = {
    val countries = (1 to 10).map({ i => Country.random }).toList.sequence
    val hashtags = (1 to 10).map({ i => Hashtag.random }).toList.sequence

    (names.takeRandom, countries, hashtags).mapN({ (randomName, randomCountries, randomHashtags) =>
      val uid = Random.nextInt(10000).toLong + 1
      User(
        uid,
        randomName,
        "${uid}/{z}/{x}/{y}.mvt",
        Random.nextInt(10000),
        Random.nextInt(10000),
        Random.nextInt(10000),
        Random.nextDouble * 10000,
        Random.nextInt(10000),
        Random.nextDouble * 10000,
        Random.nextDouble * 10000,
        Random.nextInt(10000),
        Random.nextInt(10000),
        Random.nextInt(10000),
        Random.nextInt(10000),
        (1 to 10).map({ i => Day(new java.util.Date(scala.util.Random.nextInt(Int.MaxValue).toLong + 1199999999999L).toInstant, Random.nextInt(10)) }).toList,
        randomCountries,
        randomHashtags
      )
    })
  }
}
