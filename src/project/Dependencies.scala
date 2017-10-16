import sbt._

object Dependencies {
  val decline      = "com.monovore"                %% "decline"                     % Version.decline
  val hive         = "org.apache.spark"            %% "spark-hive"                  % Version.hive % "provided"
  val gtGeomesa    = "org.locationtech.geotrellis" %% "geotrellis-geomesa"          % Version.geotrellis
  val gtGeotools   = "org.locationtech.geotrellis" %% "geotrellis-geotools"         % Version.geotrellis
  val gtS3         = "org.locationtech.geotrellis" %% "geotrellis-s3"               % Version.geotrellis
  val gtSpark      = "org.locationtech.geotrellis" %% "geotrellis-spark"            % Version.geotrellis
  val gtVector     = "org.locationtech.geotrellis" %% "geotrellis-vector"           % Version.geotrellis
  val gtVectorTile = "org.locationtech.geotrellis" %% "geotrellis-vectortile"       % Version.geotrellis
  val vectorpip    = "com.azavea"                  %% "vectorpipe"                  % Version.vectorpipe
  val cats         = "org.typelevel"               %% "cats"                        % Version.cats
  val scalactic    = "org.scalactic"               %% "scalactic"                   % Version.scalactic
  val scalatest    = "org.scalatest"               %%  "scalatest"                  % Version.scalatest % "test"
}
