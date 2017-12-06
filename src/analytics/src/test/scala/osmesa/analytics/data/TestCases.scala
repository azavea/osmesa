package osmesa.analytics.data

import geotrellis.util.Haversine

object TestCases {
  import TestData._

  val ROAD_TAG = Map("highway" -> "motorway")

  /* One person creates a road, and then someone else moves a node in that road.
   * This should cause 1 road added and 1 road modified.
   */
  def createWayThenNodeChange: OsmDataset =
    OsmDataset.build { (elements, changes) =>
      val node1 =
        elements.newNode(-102.2277, 21.7492, Map(), Some("node1"))

      val node2 =
        elements.newNode(-98.2397, 20.0662, Map(), Some("node2"))

      val node2e =
        node2.copy(lon = -98.23)

      val way =
        elements.newWay(
          Seq(node1, node2),
          ROAD_TAG,
          None
        )

      changes.add {
        Changeset(
          User.Bob,
          Seq(Hashtag.Mapathon),
          Seq(way)
        )
      }

      changes.add {
        Changeset(
          User.Alice,
          Seq(),
          Seq(node2e)
        )
      }

      val d1 = Haversine(node1.lon, node1.lat, node2.lon, node2.lat)
      val d2 = Haversine(node1.lon, node2.lat, node2e.lon, node2e.lat)

      Some(
        (
          Seq(
            ExpectedUserStats(
              User.Bob,
              roads = ExpectedCounts(1, 0),
//              roadsKm = ExpectedLengths(d1, 0.0),
              hashtags = Set(Hashtag.Mapathon),
              countries = Set("Mexico")
            ),
            ExpectedUserStats(User.Alice, roads = ExpectedCounts(0, 1), countries = Set("Mexico"))
          ),
          Seq(
            ExpectedHashtagStats(
              Hashtag.Mapathon,
              roads = ExpectedCounts(1, 0),
//              roadsKm = ExpectedLengths(0.0, math.abs(d2 - d1)),
              users = Set(User.Bob),
              totalEdits = 1
            )
          )
        )
      )
    }

  def story1: OsmDataset =
    OsmDataset.build { (elements, changes) =>
      changes.add {
        Changeset(
          User.Alice,
          Seq(Hashtag.GoEagles),
          Seq(
            elements.newWay(Geometries.eaglesField.exterior, Map("building" -> "yes"), Some("eaglesField"))
          )
        )
      }

      None
    }
}
