package osmesa.common.model

import osmesa.common.ProcessOSM.{NodeType, RelationType, WayType}

import scala.xml.Node

case class Member(`type`: Byte, ref: Long, role: String)

object Member {
  def typeFromString(str: String): Byte = str match {
      case "node"     => NodeType
      case "way"      => WayType
      case "relation" => RelationType
  }

  def fromXML(node: Node): Member = {
    val `type` = typeFromString(node \@ "type")
    val ref = (node \@ "ref").toLong
    val role = node \@ "role"

    Member(`type`, ref, role)
  }
}
