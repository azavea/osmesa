import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.azavea",
  version := Version.osmesa,
  cancelable in Global := true,
  scalaVersion := Version.scala,
  scalacOptions := Seq(
    "-deprecation",
    "-unchecked",
    "-feature",
    "-language:implicitConversions",
    "-language:reflectiveCalls",
    "-language:higherKinds",
    "-language:postfixOps",
    "-language:existentials",
    "-language:experimental.macros",
    "-feature",
    "-Ypartial-unification",
    "-Ypatmat-exhaust-depth", "100"
  ),
  resolvers ++= Seq(
    Resolver.sonatypeRepo("snapshots"),
    Resolver.bintrayRepo("lonelyplanet", "maven"),
    Resolver.bintrayRepo("kwark", "maven"), // Required for Slick 3.1.1.2, see https://github.com/azavea/raster-foundry/pull/1576
    Resolver.bintrayRepo("bkirwi", "maven"), // Required for `decline` dependency
    Resolver.mavenLocal,
    "locationtech-releases" at "https://repo.locationtech.org/content/repositories/releases/",
    "locationtech-snapshots" at "https://repo.locationtech.org/content/repositories/snapshots/",
    "boundlessgeo" at "http://repo.boundlessgeo.com/main/",
    "geosolutions" at "http://maven.geo-solutions.it/",
    "osgeo" at "http://download.osgeo.org/webdav/geotools/"
  ),
  shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
)

/* Allow `run` to be used with Spark code, while assembling fat JARs w/o Spark bundled */
// run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
// runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated

lazy val root = Project("osmesa", file("."))
  .aggregate(
    common,
    ingest
  ).settings(commonSettings: _*)

lazy val common = project.settings(commonSettings: _*)

lazy val ingest = project
  .settings(commonSettings: _*)
  .dependsOn(common)

// run --orc s3://vectortiles/orc/europe/andorra.orc --bucket vectortiles --key orc-catalog --layer andorra --local
// run --orc /home/colin/code/playground/scala/orc/ireland.orc --bucket vectortiles --key orc-catalog --layer ireland --local
// run --orc /home/colin/code/azavea/vp-io-test/georgia.orc --bucket vectortiles --key orc-catalog --layer georgia --local
