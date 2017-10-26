# data "template_file" "emr_configurations" {
#   template = "${file("../emr-configurations.json")}"
# }

# module "emr" {
#   source = "github.com/azavea/terraform-aws-emr-cluster?ref=0.1.0"

#   # TODO: Make vars
#   name          = "Osmesa Analytics"
#   vpc_id        = "${aws_vpc.dataproc.id}"
#   release_label = "emr-5.8.0"

#   applications = [
#     "Hadoop",
#     "Ganglia",
#     "Spark",
#     "Zeppelin",
#   ]

#   configurations  = "${data.template_file.emr_configurations.rendered}"
#   key_name        = "${var.aws_key_name}"
#   subnet_id       = "${element(aws_subnet.public.*.id, 0)}"
#   instance_groups = "${var.emr_instance_groups}"
#   bootstrap_name  = "runif"
#   bootstrap_uri   = "s3://elasticmapreduce/bootstrap-actions/run-if"
#   bootstrap_args  = ["instance.isMaster=true", "echo running on master node"]
#   log_uri         = "${var.emr_log_uri}"

#   project     = "${var.project}"
#   environment = "${var.environment}"
# }

# `aws_emr_cluster` is built-in to Terraform. We name ours `emrSparkCluster`.
resource "aws_emr_cluster" "emrSparkCluster" {
  name          = "Osmesa Analytics"
  release_label = "emr-5.8.0"            # 2017 August

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
    instance_count = 20
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
