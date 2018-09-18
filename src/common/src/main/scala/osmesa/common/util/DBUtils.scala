package osmesa.common.util

import java.net.URI
import java.sql.{Connection, DriverManager}

object DBUtils {
  def getJdbcConnection(uri: URI): Connection = {
    val auth = Auth.fromUri(uri)
    (auth.user, auth.password) match {
      case (Some(user), Some(pass)) =>
        DriverManager.getConnection(s"jdbc:${uri.toString}", user, pass)
      case _ =>
        DriverManager.getConnection(s"jdbc:${uri.toString}")
    }
  }
}
