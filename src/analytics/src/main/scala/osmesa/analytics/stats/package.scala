package osmesa.analytics

package object stats {
  type StatTopic = String

  def mergeMaps[K, V](m1: Map[K, Set[V]], m2: Map[K, Set[V]]): Map[K, Set[V]] =
    (m1.toSeq ++ m2.toSeq).
      groupBy(_._1).
      map { case (k, vs) =>
        (k, vs.map(_._2).reduce(_ ++ _))
      }.
      toMap
}
