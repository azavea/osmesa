package osmesa.stats

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.WebMercator
import geotrellis.raster._
import geotrellis.spark.tiling._


object TileLayouts extends LazyLogging {
  private val layouts: Array[LayoutDefinition] = (0 to 30).map({ n =>
    ZoomedLayoutScheme.layoutForZoom(n, WebMercator.worldExtent, 256)
  }).toArray

  def apply(i: Int) = layouts(i)
}

