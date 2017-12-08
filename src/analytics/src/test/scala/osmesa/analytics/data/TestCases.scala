package osmesa.analytics.data

import osmesa.analytics.stats._

object TestCases {
  import TestData._

  val ROAD_TAG = Map("highway" -> "motorway")

  def apply(): Map[String, OsmDataset] =
    Map(
      /* One person creates a road, and then someone else moves a node in that road.
       * This should cause 1 road added and 1 road modified.
       */
      "create way then node change" ->
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

          val d1 = Distance.kmBetween(node1.lon, node1.lat, node2.lon, node2.lat)
          val d2 = math.abs(Distance.kmBetween(node1.lon, node1.lat, node2e.lon, node2e.lat) - d1)

          Some(
            (
              Seq(
                ExpectedUserStats(
                  User.Bob,
                  roads = (1, 0),
                  roadsKm = (d1, 0.0),
                  hashtags = Set(Hashtag.Mapathon),
                  countries = Set("Mexico")
                ),
                ExpectedUserStats(
                  User.Alice,
                  roads = (0, 1),
                  roadsKm = (0.0, d2),
                  countries = Set("Mexico")
                )
              ),
              Seq(
                ExpectedHashtagStats(
                  Hashtag.Mapathon,
                  roads = (1, 0),
                  roadsKm = (d1, 0.0),
                  users = Set(User.Bob),
                  totalEdits = 1
                )
              )
            )
          )
        },

      /* One person creates a road, and then someone else moves two nodes in that road.
       * This should cause 1 road added and 1 road modified.
       */

      "create way then 2 nodes change" ->
        OsmDataset.build { (elements, changes) =>
          val node1 =
            elements.newNode(75.8935, 18.4587, Map(), Some("node1"))

          val node2 =
            elements.newNode(78.75, 20.52993, Map(), Some("node2"))

          val node3 =
            elements.newNode(80.3759, 17.9996, Map(), Some("node3"))

          val node2e =
            node2.copy(lon = 78.65)

          val node3e =
            node3.copy(lon = 80.4759)

          val way =
            elements.newWay(
              Seq(node1, node2, node3),
              ROAD_TAG,
              None
            )

          changes.add {
            Changeset(
              User.Bob,
              Seq(),
              Seq(way)
            )
          }

          changes.add {
            Changeset(
              User.Alice,
              Seq(Hashtag.Mapathon),
              Seq(node2e, node3e)
            )
          }

          val d1 =
            Distance.kmBetween(node1.lon, node1.lat, node2.lon, node2.lat) +
          Distance.kmBetween(node2.lon, node2.lat, node3.lon, node3.lat)

          val d2 =
            math.abs(
              Distance.kmBetween(node1.lon, node1.lat, node2e.lon, node2e.lat) +
                Distance.kmBetween(node2e.lon, node2e.lat, node3e.lon, node3e.lat) -
                d1
            )

          Some(
            (
              Seq(
                ExpectedUserStats(
                  User.Bob,
                  roads = (1, 0),
                  roadsKm = (d1, 0.0),
                  countries = Set("India")
                ),
                ExpectedUserStats(
                  User.Alice,
                  roads = (0, 1),
                  roadsKm = (0.0, d2),
                  hashtags = Set(Hashtag.Mapathon),
                  countries = Set("India")
                )
              ),
              Seq(
                ExpectedHashtagStats(
                  Hashtag.Mapathon,
                  roads = (0, 1),
                  roadsKm = (0.0, d2),
                  users = Set(User.Alice),
                  totalEdits = 1
                )
              )
            )
          )
        },

      /* One person creates a node in Mexico. Then another person moved that node.
       * Should result in a country edit for each of them.
       */

      "modify node counts as country edit" ->
        OsmDataset.build { (elements, changes) =>

          val node1 =
            elements.newNode(-98.2397, 20.0662, Map(), Some("node2"))

          val node1e =
            node1.copy(lon = -98.23)

          changes.add {
            Changeset(
              User.Bob,
              Seq(),
              Seq(node1)
            )
          }

          changes.add {
            Changeset(
              User.Alice,
              Seq(),
              Seq(node1e)
            )
          }

          Some(
            (
              Seq(
                ExpectedUserStats(
                  User.Bob,
                  countries = Set("Mexico")
                ),
                ExpectedUserStats(
                  User.Alice,
                  countries = Set("Mexico")
                )
              ),
              Seq()
            )
          )
        },

      // Changing the metadata of a way, without changing any nodes,
      // should still count as a country edit.
      "metadata only change adds to country" ->
        OsmDataset.build { (elements, changes) =>
          val node1 =
            elements.newNode(-102.2277, 21.7492, Map(), Some("node1"))

          val node2 =
            elements.newNode(-98.2397, 20.0662, Map(), Some("node2"))

          val way1 =
            elements.newWay(
              Seq(node1, node2),
              ROAD_TAG,
              None
            )

          val way1e =
            way1.copy(tags = Map("highway" -> "trunk"))

          changes.add {
            Changeset(
              User.Bob,
              Seq(),
              Seq(way1)
            )
          }

          changes.add {
            Changeset(
              User.Alice,
              Seq(),
              Seq(way1e)
            )
          }

          val d1 = Distance.kmBetween(node1.lon, node1.lat, node2.lon, node2.lat)

          Some(
            (
              Seq(
                ExpectedUserStats(
                  User.Bob,
                  roads = (1, 0),
                  roadsKm = (d1, 0.0),
                  countries = Set("Mexico")
                ),
                ExpectedUserStats(
                  User.Alice,
                  roads = (0, 1),
                  roadsKm = (0.0, 0.0),
                  countries = Set("Mexico")
                )
              ),
              Seq()
            )
          )
        },

      // Adding a way of previously committed nodes.
      "adding a way of previous nodes" ->
        OsmDataset.build { (elements, changes) =>
          val node1 =
            elements.newNode(-102.2277, 21.7492, Map(), Some("node1"))

          val node2 =
            elements.newNode(-98.2397, 20.0662, Map(), Some("node2"))

          val way =
            elements.newWay(
              Seq(node1, node2),
              ROAD_TAG,
              None
            )

          changes.add {
            Changeset(
              User.Bob,
              Seq(),
              Seq(node1, node2)
            )
          }

          changes.add {
            Changeset(
              User.Alice,
              Seq(),
              Seq(way)
            )
          }

          val d1 = Distance.kmBetween(node1.lon, node1.lat, node2.lon, node2.lat)

          Some(
            (
              Seq(
                ExpectedUserStats(
                  User.Bob,
                  countries = Set("Mexico")
                ),
                ExpectedUserStats(
                  User.Alice,
                  roads = (1, 0),
                  roadsKm = (d1, 0.0),
                  countries = Set("Mexico")
                )
              ),
              Seq()
            )
          )
        }

      // Invalid data test cases
    )
}
