package osmesa.analytics.data

object TestCases {
  import TestData._

  val roadTag = Map("highway" -> "motorway")

  /* One person creates a road, and then someone else moves a node in that road.
   * This should cause 1 road added and 1 road modified.
   */
  def createWayThenNodeChange: OsmDataset =
    OsmDataset.build { (elements, changes) =>
      val node1 =
        elements.newNode(-98.77, 27.21, Map(), Some("node1"))

      val node2 =
        elements.newNode(-98.21, 26.69, Map(), Some("node2"))

      val way =
        elements.newWay(
          Seq(node1, node2),
          roadTag,
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
          Seq(),
          Seq(node2.copy(lon = -98.23))
        )
      }

      Some(
        (
          Seq(
            ExpectedUserStats(User.Bob, roads = ExpectedCounts(1, 0), countries = Set("mexico")),
            ExpectedUserStats(User.Alice, roads = ExpectedCounts(0, 1), countries = Set("mexico"))
          ),
          Seq()
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
