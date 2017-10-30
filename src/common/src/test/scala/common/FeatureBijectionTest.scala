package osmesa.common

import geotrellis.vector._
import geotrellis.geotools._
import geotrellis.geomesa.geotools._
import org.scalatest._
import org.geotools.feature.simple.SimpleFeatureImpl

import scala.util.Try

case class ElementData(meta: ElementMeta, tagMap: Map[String, String])

/** All Element types have these attributes in common. */
case class ElementMeta(
  id: Long,
  user: String,
  userId: String,
  changeSet: Long,
  version: Long,
  timestamp: String,
  visible: Boolean
)

class FeatureBijectionSpec extends FunSpec with Matchers {
  implicit val elementDataToMap:  ElementData => Seq[(String, Any)] = { ed =>
    (Map(
      "id" -> ed.meta.id,
      "user" -> ed.meta.user,
      "userId" -> ed.meta.userId,
      "changeSet" -> ed.meta.changeSet,
      "version" -> ed.meta.version,
      "timestamp" -> ed.meta.timestamp,
      "visible" -> ed.meta.visible
    ) ++ ed.tagMap).toSeq.asInstanceOf[Seq[(String, Any)]]
  }

  val mapToElementData: Map[String, Any] => ElementData = { map =>
    val tagMap = map.filter({ case (key, value) =>
      key match {
        case "id" => false
        case "user" => false
        case "userId" => false
        case "changeSet" => false
        case "version" => false
        case "timestamp" => false
        case "visible" => false
        case _ => true
      }
    }).asInstanceOf[Map[String, String]]

    val meta = ElementMeta(
      map("id").asInstanceOf[Long],
      map("user").asInstanceOf[String],
      map("userId").asInstanceOf[String],
      map("changeSet").asInstanceOf[Long],
      map("version").asInstanceOf[Long],
      map("timestamp").asInstanceOf[String],
      map("visible").asInstanceOf[Boolean]
    )

    ElementData(meta, tagMap.toMap)
  }

  val point = Point(1, 2)
  val feature = Feature(point, ElementData(ElementMeta(1, "user123", "userId123", 123, 12345, "some time", true), Map("someTag" -> "someValue")))

  describe("Feature/SimpleFeature conversion") {
    it("should convert from GT feature to simplefeature") {
      feature.toSimpleFeature("testFeature") shouldBe a [SimpleFeatureImpl]
    }

    it("should convert from simplefeature to GT feature") {
      val simple = feature.toSimpleFeature("testFeature")
      val feat = SimpleFeatureToFeature(simple)
      Feature(feat.geom, mapToElementData(feat.data)) shouldBe (feature)

    }
  }
}
