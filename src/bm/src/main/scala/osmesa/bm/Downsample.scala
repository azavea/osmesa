package osmesa.bm

import geotrellis.vector._

import org.apache.spark.rdd.RDD

import vectorpipe.osm._

import monocle.macros.GenLens
import com.vividsolutions.jts.algorithm.Centroid


object Downsample {

  val tags = GenLens[vectorpipe.osm.ElementMeta](_.tags)

  def transmute(rdd: RDD[OSMFeature]) = {
    rdd.map({ f =>
      val geom = {
        val _geom = Centroid.getCentroid(f.geom.jtsGeom)
        Point(_geom.x, _geom.y)
      }
      val data = tags.set(f.data.tags + ("multiplicity" -> 1.toString))(f.data)
      new OSMFeature(geom, data)
    })
  }

  private def getAddress(f: OSMFeature, zoom: Int): (Double, Double) = {
    f.geom match {
      case p: Point =>
        val u: Double = (p.x + 180.0)/360.0
        val v: Double = (p.y + 90.0)/180.0
        val x: Long = java.lang.Double.doubleToRawLongBits(u) >> (48-zoom)
        val y: Long = java.lang.Double.doubleToRawLongBits(v) >> (48-zoom)
        (x, y)
      case _ => throw new Exception
    }
  }

  def apply(rdd: RDD[OSMFeature], zoom: Int) = {
    rdd
      .map({ f => (getAddress(f, zoom), f) })
      .reduceByKey({ case (f1: OSMFeature, f2: OSMFeature) =>
        val mult1 = f1.data.tags.getOrElse("multiplicity", throw new Exception).toInt
        val mult2 = f2.data.tags.getOrElse("multiplicity", throw new Exception).toInt
        val geom = f1.geom
        val data = tags.set(f1.data.tags + ("multiplicity" -> (mult1+mult2).toString))(f1.data)
        new OSMFeature(geom, data)
      })
      .values
  }

}
