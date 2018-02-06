package osmesa.analytics

import com.vividsolutions.jts.geom.Coordinate
import geotrellis.vector._
import geotrellis.vector.io._
import org.scalatest._
import spray.json._

import geotrellis.spark.util._

class CountriesTest extends FunSuite with Matchers {
  def time[T](msg: String)(f: => T) = {
    val start = System.currentTimeMillis
    val v = f
    val end = System.currentTimeMillis
    println(s"[TIMING] ${msg}: ${java.text.NumberFormat.getIntegerInstance.format(end - start)} ms")
    v
  }

  def write(path: String, txt: String): Unit = {
    import java.nio.file.{Paths, Files}
    import java.nio.charset.StandardCharsets

    Files.write(Paths.get(path), txt.getBytes(StandardCharsets.UTF_8))
  }

  test("Generate some random points and see if they make sense") {
    val countries = Countries.all
    val rand = new scala.util.Random
    val points =
      countries.flatMap { mpf =>
        val env = mpf.geom.envelope

        for(i <- 0 until 10) yield {
          val x = env.xmin + (rand.nextDouble * env.width)
          val y = env.ymin + (rand.nextDouble * env.height)
          new Coordinate(x, y)
        }
      }

    val l = {
      // Ensure that we can serialize the Lookup.
      val x =
        time("Creating CountryLookup") { new CountryLookup() }
      val s = KryoSerializer.serialize(x)
      KryoSerializer.deserialize[CountryLookup](s)
    }

    val pcs =
      Countries.all.map { mpf =>
        (mpf.geom.prepare, mpf.data)
      }

    // Brute force lookup, without spatial index
    def bfLookup(coord: Coordinate): Option[CountryId] =
      pcs.find { case (pg, _) => pg.contains(Point(coord.x, coord.y)) }.
        map { case (_, data) => data }

    val actual =
      time("LOOKUP") {
        points.
          map { p => l.lookup(p).map { cid => PointFeature(Point(p.x, p.y), cid) } }
      }

    val expected =
      time("BRUTE FORCE LOOKUP") {
        points.
          map { p =>
            bfLookup(p).map { cid => PointFeature(Point(p.x, p.y), cid) }
          }
      }

    val nodeIndex =
      time("Creating nodeIndex") {
        SpatialIndex(points) { p => (p.x, p.y) }
      }

    val nodeIndexed =
      time("NODE INDEX LOOKUP") {
        // Another way to do the spatial index, indexing the nodes instead of the countries.
        // This turns out to be slower than the lookup for large point sets.
        val result: Vector[Option[PointFeature[CountryId]]] =
          Countries.all.
            flatMap { mpf =>
              val pg = mpf.geom.prepare
              nodeIndex.traversePointsInExtent(mpf.geom.envelope).
                map { p =>
                  if(pg.covers(p)) { Some(PointFeature(Point(p.x, p.y), mpf.data)) }
                  else { None }
                }
            }
        result
      }

    actual.flatten.length should be (expected.flatten.length)
    actual.flatten.length should be (nodeIndexed.flatten.length)
  }
}
