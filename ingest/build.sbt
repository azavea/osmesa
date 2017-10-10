name := """osmesa-ingest"""

version := "0.1.0"

scalaVersion := "2.11.11"

/* To massage `cats` a little bit */
scalacOptions := Seq(
  "-Ypartial-unification"
)

/* For `decline` dependency */
resolvers += Resolver.bintrayRepo("bkirwi", "maven")

/* Local Maven Repository */
resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "com.azavea"                  %% "vectorpipe"                  % "1.0.0-SNAPSHOT",
  "com.monovore"                %% "decline"                     % "0.3.0",
  "org.apache.spark"            %% "spark-hive"                  % "2.2.0" % "provided",
  "org.locationtech.geotrellis" %% "geotrellis-s3"               % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-spark"            % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-vector"           % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-vectortile"       % Version.geotrellis,
  "org.typelevel"               %% "cats"                        % "0.9.0",

  "org.locationtech.geomesa" %% "geomesa-hbase-datastore" % "1.4.0-SNAPSHOT",
  "org.scalatest"              %%  "scalatest"               % "3.0.3" % "test"
)

/* Allow `run` to be used with Spark code, while assembling fat JARs w/o Spark bundled */
// run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
// runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated

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
// run --orc s3://vectortiles/orc/europe/andorra.orc --bucket vectortiles --key orc-catalog --layer andorra --local
// run --orc /home/colin/code/playground/scala/orc/ireland.orc --bucket vectortiles --key orc-catalog --layer ireland --local
// run --orc /home/colin/code/azavea/vp-io-test/georgia.orc --bucket vectortiles --key orc-catalog --layer georgia --local
