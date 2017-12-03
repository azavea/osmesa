package osmesa.analytics.data

object TestCases {
  import TestData._

  def story1: OsmDataset =
    OsmDataset.build { (elements, changes) =>
      changes.add {
        Changeset(
          User.Alice,
          Seq(Hashtag.GoEagles),
          Seq(
            WayChange(
              elements.newWay(Geometries.eaglesField.exterior, Map("building" -> "yes"), Some("eaglesField"))
            )
          )
        )
      }
    }
}
