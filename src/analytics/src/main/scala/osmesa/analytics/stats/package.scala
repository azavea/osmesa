package osmesa.analytics

package object stats {
  type StatTopic = String

  def mergeSetMaps[K, V](m1: Map[K, Set[V]], m2: Map[K, Set[V]]): Map[K, Set[V]] =
    (m1.toSeq ++ m2.toSeq).
      groupBy(_._1).
      map { case (k, vs) =>
        (k, vs.map(_._2).reduce(_ ++ _))
      }.
      toMap

  def mergeIntMaps[K](m1: Map[K, Int], m2: Map[K, Int]): Map[K, Int] =
    (m1.toSeq ++ m2.toSeq).
      groupBy(_._1).
      map { case (k, vs) =>
        (k, vs.map(_._2).sum)
      }.
      toMap

}
