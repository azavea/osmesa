package osmesa.common.model

import osmesa.common.ProcessOSM.{NodeType, RelationType, WayType}

import scala.xml.Node

case class Member(`type`: Byte, ref: Long, role: String)

object Member {
  def fromXML(node: Node): Member = {
    val `type` = node \@ "type" match {
      case "node"     => NodeType
      case "way"      => WayType
      case "relation" => RelationType
    }
    val ref = (node \@ "ref").toLong
    val role = node \@ "role"

    Member(`type`, ref, role)
  }
}
