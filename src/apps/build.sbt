import Dependencies._

name := "osmesa-apps"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.7"

libraryDependencies ++= Seq(
  // Deal with GeoMesa dependency that breaks Spark 2.2
  "org.json4s" %% "json4s-native" % "3.2.11",
  "org.json4s" %% "json4s-core" % "3.2.11",
  "org.json4s" %% "json4s-ast" % "3.2.11",

  "org.postgresql" % "postgresql" % "42.2.2",

  decline,
  sparkHive % Provided,
  sparkJts,
  gtGeotools exclude("com.google.protobuf", "protobuf-java"),
  gtS3 exclude("com.google.protobuf", "protobuf-java") exclude("com.amazonaws", "aws-java-sdk-s3"),
  gtSpark exclude("com.google.protobuf", "protobuf-java"),
  gtVector exclude("com.google.protobuf", "protobuf-java"),
  gtVectorTile exclude("com.google.protobuf", "protobuf-java"),
  "com.google.protobuf" % "protobuf-java" % "2.5.0",
  vectorpipe exclude("com.google.protobuf", "protobuf-java"),
  cats,
  scalactic,
  gtSparkTestKit,
  logging,
  scalatest,
  apacheCommonsEmail,

  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.340" % Provided
)

/* Fixes Spark breakage with `sbt run` as of sbt-1.0.2 */
fork in run := true

fork in Test := true

test in assembly := {}

javaOptions ++= Seq("-Xmx5G")

initialCommands in console :=
  """
  """

assemblyJarName in assembly := "osmesa-apps.jar"

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

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
