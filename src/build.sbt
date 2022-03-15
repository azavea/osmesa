import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.azavea",
  version := Version.osmesa,
  cancelable in Global := true,
  scalaVersion in ThisBuild := Version.scala,
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

  // resolvers ++= Seq(
  //   "locationtech-releases" at "https://repo.locationtech.org/content/repositories/releases/",
  //   "locationtech-snapshots" at "https://repo.locationtech.org/content/repositories/snapshots/",
  //   "geosolutions" at "http://maven.geo-solutions.it/",
  //   "osgeo-releases" at "https://repo.osgeo.org/repository/release/",
  //   "apache.commons.io" at "https://mvnrepository.com/artifact/commons-io/commons-io"
  // ),
  externalResolvers := Settings.Repositories.all,

  updateOptions := updateOptions.value.withGigahorse(false),
  shellPrompt := { s => Project.extract(s).currentProject.id + " > " },
  assemblyMergeStrategy in assembly := {
    case "reference.conf" | "application.conf"  => MergeStrategy.concat
    case PathList("META-INF", xs@_*) =>
      xs match {
        case ("MANIFEST.MF" :: Nil) => MergeStrategy.discard
        // Concatenate everything in the services directory to keep GeoTools happy.
        case ("services" :: _ :: Nil) =>
          MergeStrategy.concat
        // Concatenate these to keep JAI happy.
        case ("javax.media.jai.registryFile.jai" :: Nil) | ("registryFile.jai" :: Nil) | ("registryFile.jaiext" :: Nil) =>
          MergeStrategy.concat
        case (name :: Nil) => {
          // Must exclude META-INF/*.([RD]SA|SF) to avoid "Invalid signature file digest for Manifest main attributes" exception.
          if (name.endsWith(".RSA") || name.endsWith(".DSA") || name.endsWith(".SF"))
            MergeStrategy.discard
          else
            MergeStrategy.first
        }
        case _ => MergeStrategy.first
      }
    case _ => MergeStrategy.first
  }
)

/* Allow `run` to be used with Spark code, while assembling fat JARs w/o Spark bundled */
// run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
// runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated

lazy val root = Project("osmesa", file("."))
  .aggregate(
    analytics,
    apps,
    bm
  ).settings(commonSettings: _*)

lazy val analytics =
  project
    .settings(commonSettings: _*)

lazy val apps =
  project
    .dependsOn(analytics)
    .settings(commonSettings: _*)

lazy val bm =
  project
    .settings(commonSettings: _*)

/* Run with
      jmh:run -t 1 -f 1 -wi 5 -i 5 .*Bench.*
 */
// lazy val bench =
//   project.in(file("bench"))
//     .settings(commonSettings)
//     .dependsOn(analytics)
//     .enablePlugins(JmhPlugin)
