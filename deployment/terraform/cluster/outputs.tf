output "emr-id" {
  value = "${aws_emr_cluster.emr-spark-cluster.id}"
}

output "emr-master" {
  value = "${aws_emr_cluster.emr-spark-cluster.master_public_dns}"
}

output "key-name" {
  value = "${var.key_name}"
}
