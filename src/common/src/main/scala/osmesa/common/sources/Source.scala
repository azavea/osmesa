package osmesa.common.sources

object Source {
  val AugmentedDiffs: String = "augmented-diffs"
  val Changes: String = "changes"
  val Changesets: String = "changesets"

  val BaseURI: String = "base_uri"
  val BatchSize: String = "batch_size"
  val DatabaseURI: String = "db_uri"
  val PartitionCount: String = "partition_count"
  val ProcessName: String = "proc_name"
  val StartSequence: String = "start_sequence"
  val EndSequence: String = "end_sequence"

  val ErrorCodes: Set[Int] = Set(403, 404)
}
