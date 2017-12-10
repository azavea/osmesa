resource "aws_emr_cluster" "emrSparkCluster" {
  name          = "Osmesa Analytics"
  release_label = "emr-5.8.0"            # 2017 August

  # This it will work if only `Spark` is named here, but booting the cluster seems
  # to be much faster when `Hadoop` is included. Ingests, etc., will succeed
  # even if `Hadoop` is missing here.
  applications = ["Hadoop", "Spark", "Zeppelin", "HBase", "Ganglia"]

  ec2_attributes {
    instance_profile = "EMR_EC2_DefaultRole" # This seems to be the only necessary field.
    key_name         = "${var.key_name}"
    subnet_id        = "${var.subnet_id}"
  }

  # MASTER group must have an instance_count of 1.
  # `xlarge` seems to be the smallest type they'll allow (large didn't work).
  instance_group {
    bid_price      = "0.10"
    instance_count = 1
    instance_role  = "MASTER"
    instance_type  = "m3.xlarge"
    name           = "OsmesaAnalytics-MasterGroup"
  }

  instance_group {
    bid_price      = "0.10"
    instance_count = "${var.slave_count}"
    instance_role  = "CORE"
    instance_type  = "m3.xlarge"
    name           = "OsmesaAnalytics-CoreGroup"
  }

  # Location to dump logs
  log_uri = "${var.s3_uri}"

  tags {
    name = "OSMesa Analytics Cluster"
    role = "EMR_DefaultRole"
    env  = "env"
  }

  configurations = "deployment/terraform/emr-configurations.json"

  # bootstrap_action {
  #   path = "s3://elasticmapreduce/bootstrap-actions/setup-hbase.sh"
  #   name = "Setup HBase"
  # }

  # This is the effect of `aws emr create-cluster --use-default-roles`.
  service_role = "EMR_DefaultRole"
}

# Pipable to other programs.
output "emrID" {
  value = "${aws_emr_cluster.emrSparkCluster.id}"
}
