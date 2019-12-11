package osmesa.apps

import java.net.URI
import java.sql.Connection

import vectorpipe.util.DBUtils

object DbUtils {
  /**
    * Upsert a diff sequence number to a database, tied to a unique procName String
    *
    * Must be a PostgreSQL database
    * PostgreSQL database must contain table schema:
    * `checkpoints`:
    *   - proc_name: String
    *   - sequence: Int
    *
    * @param procName
    * @param sequence
    * @param databaseURI
    * @return
    */
  def saveLocations(procName: String, sequence: Int, databaseURI: URI) = {
    var connection: Connection = null
    try {
      connection = DBUtils.getJdbcConnection(databaseURI)
      val upsertSequence =
        connection.prepareStatement(
          """
            |INSERT INTO checkpoints (proc_name, sequence)
            |VALUES (?, ?)
            |ON CONFLICT (proc_name)
            |DO UPDATE SET sequence = ?
          """.stripMargin
        )
      upsertSequence.setString(1, procName)
      upsertSequence.setInt(2, sequence)
      upsertSequence.setInt(3, sequence)
      upsertSequence.execute()
    } finally {
      if (connection != null) connection.close()
    }
  }
}
