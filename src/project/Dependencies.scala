import sbt._

object Dependencies {
  val decline      = "com.monovore"                %% "decline"                     % Version.decline
  val hive         = "org.apache.spark"            %% "spark-hive"                  % Version.hive
  val gtGeomesa    = "org.locationtech.geotrellis" %% "geotrellis-geomesa"          % Version.geotrellis
  val gtGeotools   = "org.locationtech.geotrellis" %% "geotrellis-geotools"         % Version.geotrellis
  val gtS3         = "org.locationtech.geotrellis" %% "geotrellis-s3"               % Version.geotrellis
  val gtSpark      = "org.locationtech.geotrellis" %% "geotrellis-spark"            % Version.geotrellis
  val gtVector     = "org.locationtech.geotrellis" %% "geotrellis-vector"           % Version.geotrellis
  val gtVectorTile = "org.locationtech.geotrellis" %% "geotrellis-vectortile"       % Version.geotrellis
  val vectorpipe   = "com.azavea"                  %% "vectorpipe"                  % Version.vectorpipe
  val cats         = "org.typelevel"               %% "cats"                        % Version.cats
  val scalactic    = "org.scalactic"               %% "scalactic"                   % Version.scalactic
  val scalatest    = "org.scalatest"               %%  "scalatest"                  % Version.scalatest % "test"
  val sparkHive    = "org.apache.spark"            %% "spark-hive"                  % Version.spark
  val sparkSql     = "org.apache.spark"            %% "spark-sql"                   % Version.spark
  val jaiCore      = "javax.media" % "jai_core"    % "1.1.3"                        % "test" from "http://download.osgeo.org/webdav/geotools/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar"
  val hbaseCommon  = "org.apache.hbase"            % "hbase-common"                 % Version.hbase
  val hbaseClient  = "org.apache.hbase"            % "hbase-client"                 % Version.hbase
  val hbaseServer  = "org.apache.hbase"            % "hbase-server"                 % Version.hbase
  val gmHBaseStore = "org.locationtech.geomesa"    % "geomesa-hbase-datastore_2.11" % Version.geomesa
  val kryo         = "com.esotericsoftware"        % "kryo-shaded"                  % Version.kryo
  val snakeyaml    = "org.yaml"                    % "snakeyaml"                    % Version.snakeyaml
  val protobuf     = "com.google.protobuf"         % "protobuf-java"                % Version.protobuf
}
