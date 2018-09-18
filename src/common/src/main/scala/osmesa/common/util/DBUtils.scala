package osmesa.common.util

import java.net.URI
import java.sql.{Connection, DriverManager}

object DBUtils {
  def getJdbcConnection(uri: URI): Connection = {
    
    val cleanUri = new URI(
      scheme = uri.getScheme, 
      authority = uri.getAuthority, // host and port
      path = uri.getPath, 
      query = null, 
      fragment = null)
    // also drops UserInfo

    val auth = Auth.fromUri(uri)
    (auth.user, auth.password) match {
      case (Some(user), Some(pass)) =>
        DriverManager.getConnection(s"jdbc:${cleanUri.toString}", user, pass)
      case _ =>
        DriverManager.getConnection(s"jdbc:${cleanUri.toString}")
    }
  }
}
