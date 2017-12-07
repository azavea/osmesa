package osmesa.analytics.stats

import geotrellis.util.Haversine

object Distance {
  /** KM between two points in EPSG:4326 using Haversine */
  def kmBetween(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double =
    Haversine(lon1, lat1, lon2, lat2) / 1000.0
}
