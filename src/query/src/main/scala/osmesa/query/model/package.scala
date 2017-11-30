package osmesa.query

import io.circe._
import cats.syntax.either._

import java.time.Instant
import java.net.URI
import scala.util.Random

package object model {

  implicit class RandomGetSeq[A](lst: Seq[A]) {
    def takeRandom: Option[A] = lst.lift(Random.nextInt(lst.size))
  }

  implicit class RandomGetArray[A](lst: Array[A]) {
    def takeRandom: Option[A] = lst.lift(Random.nextInt(lst.size))
  }

  implicit val encodeInstant: Encoder[Instant] = Encoder.encodeString.contramap[Instant](_.toString)

  implicit val decodeInstant: Decoder[Instant] = Decoder.decodeString.emap { str =>
    Either.catchNonFatal(Instant.parse(str)).leftMap(t => "Instant")
  }

  implicit val encodeURI: Encoder[URI] = Encoder.encodeString.contramap[URI](_.toString)

  implicit val decodeURI: Decoder[URI] = Decoder.decodeString.emap { str =>
    Either.catchNonFatal(new URI(str)).leftMap(t => "URI")
  }
}
