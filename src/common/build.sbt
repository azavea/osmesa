import Dependencies._

name := "osmesa-common"


libraryDependencies ++= Seq(
//  gtGeomesa exclude("com.google.protobuf", "protobuf-java") exclude("org.locationtech.geomesa", "geomesa-accumulo-datastore"),
  gtGeotools exclude("com.google.protobuf", "protobuf-java"),
  jaiCore,
  gtVector,
  cats,
  scalactic,
  scalatest,
  circeCore,
  circeGeneric,
  circeExtras,
  circeParser,
  circeOptics
)


initialCommands in console :=
  """
  """
