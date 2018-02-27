import Dependencies._

name := "osmesa-common"

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

libraryDependencies ++= Seq(
//  gtGeomesa exclude("com.google.protobuf", "protobuf-java") exclude("org.locationtech.geomesa", "geomesa-accumulo-datastore"),
  gtGeotools exclude("com.google.protobuf", "protobuf-java"),
  "com.github.seratch" %% "awscala"     % "0.6.1",
  "org.scalaj"         %% "scalaj-http" % "2.3.0",
  sparkHive % "provided",
  gtS3 exclude("com.google.protobuf", "protobuf-java"),
  gtSpark exclude("com.google.protobuf", "protobuf-java"),
  gtVectorTile exclude("com.google.protobuf", "protobuf-java"),
  decline,
  jaiCore,
  gtVector,
  cats,
  scalactic,
  scalatest,
  circeCore,
  circeGeneric,
  circeExtras,
  circeParser,
  circeOptics,
  circeJava8
)


initialCommands in console :=
  """
  """
