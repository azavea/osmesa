import Dependencies._

name := "osmesa-common"

libraryDependencies ++= Seq(
  "com.trueaccord.scalapb" %% "scalapb-runtime" % "0.6.6",
  hbaseClient,
  hbaseCommon,
  "org.apache.hadoop" % "hadoop-common" % "2.9.0"
)

fork in Test := true

fork in run := true
