package osmesa.common.relations
import java.sql.Timestamp

import com.vividsolutions.jts.geom.{Geometry, TopologyException}
import org.apache.log4j.Logger
import osmesa.common.ProcessOSM.WayType

object Routes {
  private lazy val logger = Logger.getLogger(getClass)

  def build(id: Long,
            version: Int,
            timestamp: Timestamp,
            types: Seq[Byte],
            roles: Seq[String],
            geoms: Seq[Geometry]): Option[Seq[(String, Geometry)]] = {
    if (types.zip(geoms).exists { case (t, g) => t == WayType && Option(g).isEmpty }) {
      // bail early if null values are present where they should exist (members w/ type=way)
      logger.debug(s"Incomplete relation: $id @ $version ($timestamp)")
      None
    } else if (types.isEmpty) {
      // empty relation
      None
    } else {

      try {
        val res = roles
          .zip(geoms.map(Option.apply))
          .filter(_._2.isDefined)
          .map(x => (x._1, x._2.get))
          .groupBy {
            case (role, _) => role
          }
          .mapValues(_.map(_._2))
          .mapValues(connectSegments)
          .map {
            case (role, lines) =>
              lines match {
                case Seq(line) => (role, line)
                case _         => (role, geometryFactory.createMultiLineString(lines.toArray))
              }
          }
          .toSeq

        Some(res)
      } catch {
        case e @ (_: AssemblyException | _: IllegalArgumentException | _: TopologyException) =>
          logger.warn(
            s"Could not reconstruct route relation $id @ $version ($timestamp): ${e.getMessage}")
          None
        case e: Throwable =>
          logger.warn(s"Could not reconstruct route relation $id @ $version ($timestamp): $e")
          e.getStackTrace.foreach(logger.warn)
          None
      }
    }
  }
}
