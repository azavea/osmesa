package osmesa.analytics.data

import osmesa.analytics.stats._

object TestCases {
  import TestData._

  val ROAD_TAG = Map("highway" -> "motorway")

  def totalDistance(nodes: Seq[Node]): Double =
    nodes.
      sliding(2).
      collect { case List(a, b) => (a, b) }.
      foldLeft(0.0) { case (acc, (n1, n2)) =>
        acc + Distance.kmBetween(n1.lon, n1.lat, n2.lon, n2.lat)
      }

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
              (Distance.kmBetween(node1.lon, node1.lat, node2e.lon, node2e.lat) +
                Distance.kmBetween(node2e.lon, node2e.lat, node3e.lon, node3e.lat)) -
                d1
            )

          val dx = (Distance.kmBetween(node1.lon, node1.lat, node2e.lon, node2e.lat) +
            Distance.kmBetween(node2e.lon, node2e.lat, node3.lon, node3.lat))
          val dy = (Distance.kmBetween(node1.lon, node1.lat, node2e.lon, node2e.lat) +
            Distance.kmBetween(node2e.lon, node2e.lat, node3e.lon, node3e.lat))

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
        },

      "a way over time" ->
        OsmDataset.build { (elements, changes) =>
          val node1 =
            elements.newNode(-83.88198852539062, 40.60978237983301, Map(), None)

          val node2 =
            elements.newNode(-83.82293701171875, 40.670222795307346, Map(), None)

          val node3 =
            elements.newNode(-83.73779296875, 40.5930995321649, Map(), None)

          val node4 =
            elements.newNode(-83.83941650390625, 40.551374198715166, Map(), None)

          val wayV1 =
            elements.newWay(
              Seq(node1, node2, node3, node4),
              ROAD_TAG,
              None
            )
          val wayV1Length = totalDistance(Seq(node1, node2, node3, node4))

          val wayV2 =
            wayV1.copy(nodes = Seq(node1, node2, node3))
          val wayV2Length = totalDistance(Seq(node1, node2, node3))

          val node5 =
            elements.newNode(-83.73710632324219, 40.59336023367942, Map(), None)

          val node6 =
            elements.newNode(-83.69556427001953, 40.60978237983301, Map(), None)

          val node7 =
            elements.newNode(-83.7158203125, 40.49865881031932, Map(), None)

          val node8 =
            elements.newNode(-83.81160736083984, 40.49056515559304, Map(), None)

          val wayV3 =
            wayV2.copy(nodes = Seq(node1, node2, node3, node5, node6, node7, node8))
          val wayV3Length = totalDistance(Seq(node1, node2, node3, node5, node6, node7, node8))

          changes.add {
            Changeset(
              User.Bob,
              Seq(),
              Seq(node1, node2)
            )
          }

          changes.add {
            Changeset(
              User.Bob,
              Seq(),
              Seq(wayV1)
            )
          }

          changes.add {
            Changeset(
              User.Alice,
              Seq(),
              Seq(wayV2)
            )
          }

          changes.add {
            Changeset(
              User.Alice,
              Seq(),
              Seq(wayV3)
            )
          }

          val aliceKmEdit =
            Seq(wayV1Length, wayV2Length, wayV3Length).
              sliding(2).
              collect { case List(a, b) => (a, b) }.
              foldLeft(0.0) { case (acc, (a, b)) => acc + math.abs(b - a) }

          Some(
            (
              Seq(
                ExpectedUserStats(
                  User.Bob,
                  roads = (1, 0),
                  roadsKm = (wayV1Length, 0.0),
                  countries = Set("United States of America")
                ),
                ExpectedUserStats(
                  User.Alice,
                  roads = (0, 2),
                  roadsKm = (0.0, aliceKmEdit),
                  countries = Set("United States of America")
                )
              ),
              Seq()
            )
          )
        }

      // Invalid data test cases
    )
}
