package osmesa.common.model

object Actions {
  type Action = Byte

  val Create: Action = 1.byteValue
  val Modify: Action = 2.byteValue
  val Delete: Action = 3.byteValue
}
