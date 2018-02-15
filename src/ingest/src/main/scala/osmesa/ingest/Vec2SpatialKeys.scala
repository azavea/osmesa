package osmesa.ingest

import geotrellis.spark.tiling._
import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.spark._
import geotrellis.raster._
import geotrellis.shapefile._

object Shapefile2Geoms {
  def apply(shapefilePath: String) =
    ShapeFileReader
      .readMultiPolygonFeatures(shapefilePath)
      .map(_.geom)
}

object Geom2SpatialKeys {
  def apply[G <: Geometry](geom: G, zoom: Int): Set[SpatialKey] = {
    val mapTransform =
      ZoomedLayoutScheme.layoutForZoom(zoom, WebMercator.worldExtent).mapTransform

    geom.as[Geometry]
      .map(mapTransform.keysForGeometry)
      .getOrElse(Set.empty)
  }

  def apply[G <: Geometry](geom: G): Set[SpatialKey] = apply(geom, 14)
}

object SpatialKeys {
  def fromResources = {
    val shapefilePath = getClass().getClassLoader()
      .getResource("Plus_Metro_20161107_proj3857/Plus_Metro_20161107_proj3857.shp")
      .getPath()

    Shapefile2Geoms(shapefilePath)
      .map(Geom2SpatialKeys(_))
      .reduce(_ union _)
  }
}

