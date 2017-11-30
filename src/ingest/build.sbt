import Dependencies._

name := "osmesa-ingest"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.7"

libraryDependencies ++= Seq(
  decline,
  hive % "provided",
  sparkHive % "provided",
  gmHBaseStore,
  kryo,
  snakeyaml,
  cats,
  hbaseClient,
  hbaseCommon,
  scalactic,
  scalatest,
  gtGeotools
    exclude("com.google.protobuf", "protobuf-java"),
  gtS3
    exclude("com.google.protobuf", "protobuf-java")
    exclude("com.amazonaws", "aws-java-sdk"),
  gtSpark
    exclude("com.google.protobuf", "protobuf-java"),
  gtVector
    exclude("com.google.protobuf", "protobuf-java"),
  gtVectorTile
    exclude("com.google.protobuf", "protobuf-java"),
  vectorpipe
    exclude("com.google.protobuf", "protobuf-java"),
  "xerces" % "xercesImpl" % "2.7.1",
  "com.trueaccord.scalapb" %% "scalapb-runtime" % "0.6.6"
)

fork in Test := true

fork in run := true

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

val meta = raw"""META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case s if s.startsWith("META-INF/services") => MergeStrategy.concat
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

