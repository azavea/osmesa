package osmesa.ingest

import org.locationtech.geomesa.utils.geotools.SftBuilder


object OsmFeatureTypes {

  def osmPointFeatureType(sftName: String) = {
    val sft = new SftBuilder()
      .longType("fid")
      .stringType("user")
      .longType("userId")
      .longType("changeSet")
      .longType("version")
      .date("created", true, true, true)
      .point("geom", true)
      .booleanType("visible")
      .mapType[String, String]("tags")
      .build(sftName)

    sft.getDescriptor("created").getUserData().put("index", "full")
    sft
  }

  def osmLineStringFeatureType(sftName: String) = {
    val sft = new SftBuilder()
      .longType("fid")
      .stringType("user")
      .longType("userId")
      .longType("changeSet")
      .longType("version")
      .date("created", true, true, true)
      .lineString("geom", true)
      .booleanType("visible")
      .mapType[String, String]("tags")
      .build(sftName)

    sft.getDescriptor("created").getUserData().put("index", "full")
    sft
  }

  def osmMultiPolyFeatureType(sftName: String) = {
    val sft = new SftBuilder()
      .longType("fid")
      .stringType("user")
      .longType("userId")
      .longType("changeSet")
      .longType("version")
      .date("created", true, true, true)
      .multiPolygon("geom", true)
      .booleanType("visible")
      .mapType[String, String]("tags")
      .build(sftName)

    sft.getDescriptor("created").getUserData().put("index", "full")
    sft
  }
}

