import Dependencies._

name := "osmesa-common"

addCompilerPlugin(
  "org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full
)

libraryDependencies ++= Seq(
  //  gtGeomesa exclude("com.google.protobuf", "protobuf-java") exclude("org.locationtech.geomesa",
  // "geomesa-accumulo-datastore"),
  gtGeotools exclude ("com.google.protobuf", "protobuf-java"),
  "com.github.seratch" %% "awscala" % "0.6.1",
  "org.scalaj" %% "scalaj-http" % "2.3.0",
  sparkHive % "provided",
  sparkJts,
  gtS3 exclude ("com.google.protobuf", "protobuf-java") exclude ("com.amazonaws", "aws-java-sdk-s3"),
  gtSpark exclude ("com.google.protobuf", "protobuf-java"),
  gtVectorTile exclude ("com.google.protobuf", "protobuf-java"),
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
  circeJava8,
  circeYaml,
  "com.softwaremill.macmemo" %% "macros" % "0.4",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.340" % "provided"
)

Test / fork := true
Test / baseDirectory := (baseDirectory.value).getParentFile
Test / parallelExecution := false
Test / testOptions += Tests.Argument("-oDF")

initialCommands in console :=
  """
  """
