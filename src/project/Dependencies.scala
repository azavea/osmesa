import sbt._

object Dependencies {
  val decline        = "com.monovore"                %% "decline"                     % Version.decline
  val sparkHive      = "org.apache.spark"            %% "spark-hive"                  % Version.spark
  val sparkStreaming = "org.apache.spark"            %% "spark-streaming"             % Version.spark
  val sparkJts       = "org.locationtech.geomesa"    %% "geomesa-spark-jts"           % Version.geomesa
  val gtGeomesa      = "org.locationtech.geotrellis" %% "geotrellis-geomesa"          % Version.geotrellis
  val gtGeotools     = "org.locationtech.geotrellis" %% "geotrellis-geotools"         % Version.geotrellis
  val gtS3           = "org.locationtech.geotrellis" %% "geotrellis-s3"               % Version.geotrellis
  val gtSpark        = "org.locationtech.geotrellis" %% "geotrellis-spark"            % Version.geotrellis
  val gtSparkTestKit = "org.locationtech.geotrellis" %% "geotrellis-spark-testkit"    % Version.geotrellis % "test"
  val gtVector       = "org.locationtech.geotrellis" %% "geotrellis-vector"           % Version.geotrellis
  val gtShapefile    = "org.locationtech.geotrellis" %% "geotrellis-shapefile"        % Version.geotrellis
  val gtVectorTile   = "org.locationtech.geotrellis" %% "geotrellis-vectortile"       % Version.geotrellis
  val vectorpipe     = "com.azavea.geotrellis"       %% "vectorpipe"                  % Version.vectorpipe
  val cats           = "org.typelevel"               %% "cats-core"                   % Version.cats
  val scalactic      = "org.scalactic"               %% "scalactic"                   % Version.scalactic
  val scalatest      = "org.scalatest"               %%  "scalatest"                  % Version.scalatest % "test"
  val jaiCore        = "javax.media" % "jai_core" % "1.1.3" % "test" from "http://download.osgeo.org/webdav/geotools/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar"
  val apacheCommonsEmail = "org.apache.commons" % "commons-email" % Version.apacheCommonsEmail
  val hbaseCommon    = "org.apache.hbase" % "hbase-common" % "1.3.1"
  val hbaseClient    = "org.apache.hbase" % "hbase-client" % "1.3.1"
  val hbaseServer    = "org.apache.hbase" % "hbase-server" % "1.3.1"
  val geomesaHbaseDatastore = "org.locationtech.geomesa" % "geomesa-hbase-datastore_2.11" % Version.geomesa
  val kryo           = "com.esotericsoftware"        % "kryo-shaded"                   % Version.kryo
  val snakeyaml      = "org.yaml"                    % "snakeyaml"                     % Version.snakeyaml
  val circeCore      = "io.circe"                    %% "circe-core"                   % Version.circe
  val circeGeneric   = "io.circe"                    %% "circe-generic"                % Version.circe
  val circeExtras    = "io.circe"                    %% "circe-generic-extras"         % Version.circe
  val circeParser    = "io.circe"                    %% "circe-parser"                 % Version.circe
  val circeOptics    = "io.circe"                    %% "circe-optics"                 % Version.circe
  val circeJava8     = "io.circe"                    %% "circe-java8"                  % Version.circe
  val circeYaml      = "io.circe"                    %% "circe-yaml"                   % "0.8.0"
  val logging        = "com.typesafe.scala-logging"  %% "scala-logging"                % Version.scalaLogging
  val commonsIO      = "commons-io"                  %  "commons-io"                   % Version.commonsIO
}
