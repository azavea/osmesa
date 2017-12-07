package osmesa.analytics

package object stats {
  type StatTopic = String

  def mergeMaps[K, V](m1: Map[K, V], m2: Map[K, V])(f: (V, V) => V): Map[K, V] =
    (m1.toSeq ++ m2.toSeq).
      groupBy(_._1).
      map { case (k, vs) =>
        (k, vs.map(_._2).reduce(f))
      }.
      toMap
}
