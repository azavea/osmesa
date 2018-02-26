import Dependencies._

name := "bm-standalone"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.7"

libraryDependencies ++= Seq(
  decline,
  sparkHive % "provided",
  gtGeotools exclude("com.google.protobuf", "protobuf-java"),
  gtS3 exclude("com.google.protobuf", "protobuf-java"),
  gtSpark exclude("com.google.protobuf", "protobuf-java"),
  gtVector exclude("com.google.protobuf", "protobuf-java"),
  gtVectorTile exclude("com.google.protobuf", "protobuf-java"),
  "com.google.protobuf" % "protobuf-java" % "2.5.0",
  vectorpipe exclude("com.google.protobuf", "protobuf-java"),
  cats,
  gtSparkTestKit,
  logging,
  scalatest,

  "org.jblas" % "jblas" % "1.2.4"
)

/* Fixes Spark breakage with `sbt run` as of sbt-1.0.2 */
fork in run := true

fork in Test := true

test in assembly := {}

javaOptions ++= Seq("-Xmx5G")

initialCommands in console :=
  """
  """

assemblyJarName in assembly := "bm-standalone.jar"

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

assemblyMergeStrategy in assembly := {
  case s if s.startsWith("META-INF/services") => MergeStrategy.concat
  case "reference.conf" | "application.conf"  => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" | "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" | "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
