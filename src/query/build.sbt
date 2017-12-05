import Dependencies._

name := "osmesa-query"

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

libraryDependencies ++= Seq(
  "com.typesafe.akka"           %% "akka-actor"           % "2.5.6",
  "com.typesafe.akka"           %% "akka-slf4j"           % "2.5.6",
  "com.typesafe.akka"           %% "akka-stream"          % "2.5.6",
  "com.typesafe.akka"           %% "akka-http"            % "10.0.10",
  "com.typesafe.akka"           %% "akka-http-testkit"    % "10.0.10",
  "ch.megard"                   %% "akka-http-cors"       % "0.2.2",
  "io.circe"                    %% "circe-core"           % "0.9.0-M2",
  "io.circe"                    %% "circe-generic"        % "0.9.0-M2",
  "io.circe"                    %% "circe-generic-extras" % "0.9.0-M2",
  "io.circe"                    %% "circe-parser"         % "0.9.0-M2",
  "io.circe"                    %% "circe-optics"         % "0.9.0-M2",
  "de.heikoseeberger"           %% "akka-http-circe"      % "1.17.0",
  "org.scalaj"                  %% "scalaj-http"          % "2.3.0",
  "com.github.seratch"          %% "awscala"              % "0.6.1",
  decline,
  hive % "provided",
  kryo,
  snakeyaml,
  "org.apache.hadoop" % "hadoop-common" % "2.9.0",
  scalactic,
  scalatest,
  gtGeotools
    exclude("com.google.protobuf", "protobuf-java"),
  gtS3
    exclude("com.google.protobuf", "protobuf-java")
    exclude("com.amazonaws", "aws-java-sdk"),
  gtVector
    exclude("com.google.protobuf", "protobuf-java"),
  gtVectorTile
    exclude("com.google.protobuf", "protobuf-java"),
  vectorpipe
    exclude("com.google.protobuf", "protobuf-java"),
  "com.trueaccord.scalapb" %% "scalapb-runtime" % "0.6.6"
)

fork in Test := true

fork in run := true

javaOptions ++= Seq("-Xmx5G")

initialCommands in console :=
  """
  """

mainClass := Some("osmesa.query.Main")

assemblyJarName in assembly := "osmesa-query.jar"

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

