import Dependencies._

name := "osmesa-common"


libraryDependencies ++= Seq(
  gtGeomesa,
  gtGeotools,
  gtVector,
  cats,
  scalactic,
  scalatest
)


initialCommands in console :=
  """
  """
