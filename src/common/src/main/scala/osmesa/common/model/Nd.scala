package osmesa.common.model
import scala.xml.Node

case class Nd(ref: Long)

object Nd {
  def fromXML(node: Node): Nd =
    Nd((node \@ "ref").toLong)
}
