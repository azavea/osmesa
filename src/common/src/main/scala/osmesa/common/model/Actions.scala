package osmesa.common.model

object Actions {
  type Action = Byte

  val Create: Action = 1.byteValue
  val Modify: Action = 2.byteValue
  val Delete: Action = 3.byteValue

  def fromString(str: String): Action =
    str.toLowerCase match {
      case "create" => Actions.Create
      case "delete" => Actions.Delete
      case "modify" => Actions.Modify
    }
}
