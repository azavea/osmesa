package osmesa

import com.vividsolutions.jts.{geom => jts}
import geotrellis.spark.tiling._
import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.spark._
import geotrellis.raster._
import geotrellis.shapefile._
import geotrellis.shapefile.ShapeFileReader.SimpleFeatureWrapper
import org.geotools.data.simple._
import org.opengis.feature.simple._
import org.geotools.data.shapefile._

import scala.collection.JavaConversions._

object Shapefile2Geoms {
  def apply(shapefilePath: String) =
    ShapeFileReader
      .readMultiPolygonFeatures(shapefilePath)
      .map(_.geom)
}

object Geom2SpatialKeys {
  def apply[G <: Geometry](geom: G, layout: LayoutDefinition): Set[SpatialKey] = {
    val mapTransform = layout.mapTransform

    geom.as[Geometry]
      .map(mapTransform.keysForGeometry)
      .getOrElse(Set.empty)
  }

  def apply[G <: Geometry](geom: G): Set[SpatialKey] = apply(geom, ZoomedLayoutScheme.layoutForZoom(14, WebMercator.worldExtent))
}

object SpatialKeys {
  def metroPlusFeatures = {
    val shapefileURL = getClass().getClassLoader()
      .getResource("Plus_Metro_20161107_proj3857/Plus_Metro_20161107_proj3857.shp")

    val ds = new ShapefileDataStore(shapefileURL)
    val ftItr: SimpleFeatureIterator = ds.getFeatureSource.getFeatures.features

    val simpleFeatures = try {
      val simpleFeatures = collection.mutable.ListBuffer[SimpleFeature]()
      while(ftItr.hasNext) simpleFeatures += ftItr.next()
      simpleFeatures.toList
    } finally {
      ftItr.close
      ds.dispose
    }

    simpleFeatures.flatMap { ft => ft.geom[jts.MultiPolygon].map(MultiPolygonFeature(_, ft.attributeMap)) }
  }

  def keysForCountry(country: String, layout: LayoutDefinition) = {
    metroPlusFeatures
      .filter(_.data("Country") == country)
      .map(_.geom)
      .map(Geom2SpatialKeys(_, layout))
      .reduce(_ union _)
  }

  def fromResources = {
    metroPlusFeatures
      .map(_.geom)
      .map(Geom2SpatialKeys(_))
      .reduce(_ union _)
  }
}
