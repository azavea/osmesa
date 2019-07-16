import Dependencies._

name := "ingest"

// dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
// dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"
// dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.7"

libraryDependencies ++= Seq(
  decline,
  spark % "provided",
  sparkSql % "provided",
  sparkJts,
  gtShapefile exclude("com.google.protobuf", "protobuf-java"),
  "com.google.protobuf" % "protobuf-java" % "2.5.0",
  vectorpipe exclude("com.google.protobuf", "protobuf-java"),
  cats,
  scalactic,
  scalatest,
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.340" % "provided"
)

fork in Test := true

javaOptions ++= Seq("-Xmx5G")

initialCommands in console :=
  """
  """

assemblyJarName in assembly := "osmesa-ingest.jar"

assemblyShadeRules in assembly := {
  val shadePackage = "com.azavea.shaded.demo"
  Seq(
    ShadeRule.rename("com.google.common.**" -> s"$shadePackage.google.common.@1")
      .inLibrary("com.azavea.geotrellis" %% "geotrellis-cassandra" % Version.geotrellis).inAll,
    ShadeRule.rename("io.netty.**" -> s"$shadePackage.io.netty.@1")
      .inLibrary("com.azavea.geotrellis" %% "geotrellis-hbase" % Version.geotrellis).inAll,
    ShadeRule.rename("com.fasterxml.jackson.**" -> s"$shadePackage.com.fasterxml.jackson.@1")
      .inLibrary("com.networknt" % "json-schema-validator" % "0.1.7").inAll,
    ShadeRule.rename("org.apache.avro.**" -> s"$shadePackage.org.apache.avro.@1")
      .inLibrary("com.azavea.geotrellis" %% "geotrellis-spark" % Version.geotrellis).inAll
  )
}
