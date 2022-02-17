import sbt._

object Settings {
  object Repositories {
    val apacheCommons   = "apache.commons.io" at "https://mvnrepository.com/artifact/commons-io/commons-io"
    val eclipseReleases = "eclipse-releases" at "https://repo.eclipse.org/content/groups/releases"
    val osgeoReleases   = "osgeo-releases" at "https://repo.osgeo.org/repository/release/"
    val geosolutions    = "geosolutions" at "https://maven.geo-solutions.it/"
    val ltReleases      = "locationtech-releases" at "https://repo.locationtech.org/content/repositories/releases/"
    val ltSnapshots     = "locationtech-snapshots" at "https://repo.locationtech.org/content/repositories/snapshots/"
    val ivy2Local       = Resolver.file("local", file(Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns)
    val mavenLocal      = Resolver.mavenLocal
    val maven           = DefaultMavenRepository
    val local           = Seq(ivy2Local, mavenLocal)
    val external        = Seq(osgeoReleases, maven, apacheCommons, eclipseReleases, geosolutions, ltReleases, ltSnapshots)
    val all             = external ++ local
  }
}
