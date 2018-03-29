resource "aws_emr_cluster" "emr-spark-cluster" {
  name          = "${var.user} - ${var.cluster-name}"
  applications  = ["Hadoop", "Spark", "Ganglia", "Zeppelin"]
  release_label = "emr-5.8.0"
  service_role  = "${var.emr_service_role}"

  ec2_attributes {
    instance_profile = "${var.emr_instance_profile}"
    key_name         = "${var.key_name}"

    emr_managed_master_security_group = "${aws_security_group.emr-cluster.id}"
    emr_managed_slave_security_group  = "${aws_security_group.emr-cluster.id}"
  }

  instance_group {
    instance_count = 1
    instance_role  = "MASTER"
    instance_type  = "m3.2xlarge"
    name           = "emr-master"
  }

  instance_group {
    bid_price      = "${var.bid_price}"
    instance_count = "${var.worker_count}"
    instance_role  = "CORE"
    instance_type  = "m3.xlarge"
    name           = "emr-worker"
  }

  configurations = "cluster-configurations.json"
}

