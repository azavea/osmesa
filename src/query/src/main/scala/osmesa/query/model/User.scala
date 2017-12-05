package osmesa.query.model

import osmesa.query.util._

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.{Decoder, Encoder}
import cats._
import cats.implicits._

import scala.util.Random
import java.time.Instant


case class User(
  uid: Long,
  name: String,
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
  changesetCount: Int,
  editCount: Int,
  editors: List[EditorCount],
  editTimes: List[DayCount],
  countryList: List[CountryCount],
  hashtags: List[HashtagCount]
)

object User {
  implicit val customConfig: Configuration = Configuration.default.withSnakeCaseMemberNames.withDefaults
  implicit val encoder: Encoder[User] = deriveEncoder
  implicit val decoder: Decoder[User] = deriveDecoder

  def random = {
    User(
      math.abs(Random.nextLong),
      Corpus.randomName,
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
      Random.nextInt(10000),
      Random.nextInt(10000),
      (1 to 10).map({ _ => EditorCount.random }).toList,
      (1 to 10).map({ _ => DayCount.random }).toList,
      (1 to 10).map({ _ => CountryCount.random }).toList,
      (1 to 10).map({ _ => HashtagCount.random }).toList
    )
  }
}

