import Dependencies._

name := "osmesa-common"


libraryDependencies ++= Seq(
  gtGeotools
    exclude("com.google.protobuf", "protobuf-java"),
  jaiCore,
  gtVector,
  cats,
  scalactic,
  scalatest
)


initialCommands in console :=
  """
  """
