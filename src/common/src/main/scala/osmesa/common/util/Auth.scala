package osmesa.common.util

import java.net.URI

case class Auth(user: Option[String], password: Option[String]) {
    def isDefined: Boolean = (user.isDefined && password.isDefined)
}

object Auth {
  def fromUri(uri: URI, userParam: String = "user", passwordParam: String = "password"): Auth = {
    val auth = getUriUserInfo(uri)
    if (auth.isDefined) {
      auth
    } else {
      val params = getUriParams(uri)
      auth.copy(
        user = auth.user.orElse(params.get(userParam)),
        password = auth.password.orElse(params.get(passwordParam))
      )
    }
  }

  /** Parse only the URI auth section */
  def getUriUserInfo(uri: URI): Auth = {
    val info = uri.getUserInfo
    if (null == info)
      Auth(None, None)
    else {
      val chunk = info.split(":")
      if (chunk.length == 1)
      Auth(Some(chunk(0)), None)
      else
      Auth(Some(chunk(0)), Some(chunk(1)))
    }
  }

  /** Parse URI parameters */
  def getUriParams(uri: URI): Map[String, String] = {
    val query = uri.getQuery
    if (null == query)
      Map.empty[String, String]
    else {
      query.split("&").map{ param =>
        val arr = param.split("=")
        arr(0) -> arr(1)
      }.toMap
    }
  }
}