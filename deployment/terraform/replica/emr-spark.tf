data "template_file" "emr_configurations" {
  template = "${file("hbase/emr-read-conf.json")}"
  vars {
    s3_hbase_root_uri = "${var.s3_hbase_root_uri}"
  }
}

resource "aws_emr_cluster" "emrSparkCluster" {
  name          = "Osmesa Analytics"
  release_label = "emr-5.9.0"            # 2017 October

  # This it will work if only `Spark` is named here, but booting the cluster seems
  # to be much faster when `Hadoop` is included. Ingests, etc., will succeed
  # even if `Hadoop` is missing here.
  applications = ["Hadoop", "Spark", "Zeppelin", "HBase"]

  ec2_attributes {
    instance_profile = "EMR_EC2_DefaultRole" # This seems to be the only necessary field.
    key_name         = "${var.key_name}"
  }

  # MASTER group must have an instance_count of 1.
  # `xlarge` seems to be the smallest type they'll allow (large didn't work).
  instance_group {
    bid_price      = "0.10"
    instance_count = 1
    instance_role  = "MASTER"
    instance_type  = "m3.xlarge"
    name           = "emrVectorPipeOrcDemo-MasterGroup"
  }

  instance_group {
    bid_price      = "0.10"
    instance_count = "${var.slave_count}"
    instance_role  = "CORE"
    instance_type  = "m3.xlarge"
    name           = "emrVectorPipeOrcDemo-CoreGroup"
  }

  # Location to dump logs
  log_uri = "${var.s3_uri}"

  tags {
    name = "OSMesa Analytics Cluster"
    role = "EMR_DefaultRole"
    env  = "env"
  }

  #bootstrap_action {
  #  path = "s3://osmesa/geomesa-bootstrap.sh"
  #  name = "geomesa-bootstrap"
  #  args = ["instance.isMaster=true", "echo running on master node"]
  #}

  configurations  = "${data.template_file.emr_configurations.rendered}"

  # This is the effect of `aws emr create-cluster --use-default-roles`.
  service_role = "EMR_DefaultRole"
}

# Pipable to other programs.
output "emrID" {
  value = "${aws_emr_cluster.emrSparkCluster.id}"
}
