import Dependencies._

name := "osmesa-ingest"


libraryDependencies ++= Seq(
  decline,
  hive,
  //gtGeomesa exclude("com.google.protobuf", "protobuf-java"),
  gtGeotools exclude("com.google.protobuf", "protobuf-java"),
  gtS3 exclude("com.google.protobuf", "protobuf-java"),
  gtSpark exclude("com.google.protobuf", "protobuf-java"),
  gtVector exclude("com.google.protobuf", "protobuf-java"),
  gtVectorTile exclude("com.google.protobuf", "protobuf-java"),
  "com.google.protobuf" % "protobuf-java" % "2.5.0",
  vectorpipe exclude("com.google.protobuf", "protobuf-java"),
  geomesaHbaseDatastore,
  "org.locationtech.geomesa" % "geomesa-security_2.11" % Version.geomesa,
  "org.locationtech.geomesa" % "geomesa-feature-common_2.11" % Version.geomesa,
  "org.locationtech.geomesa" % "geomesa-utils_2.11" % Version.geomesa,
  "org.locationtech.geomesa" % "geomesa-feature-kryo_2.11" % Version.geomesa,
  "org.locationtech.geomesa" % "geomesa-filter_2.11" % Version.geomesa,
  "org.locationtech.geomesa" % "geomesa-z3_2.11" % Version.geomesa,
  "org.locationtech.geomesa" % "geomesa-index-api_2.11" % Version.geomesa,
  cats,
  hbaseClient,
  hbaseCommon,
  scalactic,
  scalatest
)

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

assemblyMergeStrategy in assembly := {
  case s if s.startsWith("META-INF/services") => MergeStrategy.concat
  case "reference.conf" | "application.conf"  => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" | "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" | "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}
