data "template_file" "emr_configurations" {
  template = "${file("hbase/emr-write-conf.json")}"
  vars {
    s3_hbase_root_uri = "${var.s3_hbase_root_uri}"
  }
}

# bootstrap-master.sh
resource "aws_s3_bucket_object" "bootstrap-master" {
  bucket = "${var.bootstrap_bucket}"
  key    = "${var.bootstrap_prefix}/bootstrap-master.sh"
  source = "../bootstrap-master.sh"
  etag   = "${md5(file("../bootstrap-master.sh"))}"
}

resource "aws_s3_bucket_object" "ingest-jar" {
  bucket = "${var.bootstrap_bucket}"
  key    = "${var.bootstrap_prefix}/osmesa-injest.jar"
  source = "../../../src/ingest/target/scala-2.11/osmesa-ingest.jar"
  etag   = "${md5(file("../../../src/ingest/target/scala-2.11/osmesa-ingest.jar"))}"
}

resource "aws_emr_cluster" "emrSparkCluster" {
  name          = "Osmesa Analytics"
  release_label = "emr-5.9.0"            # 2017 October

  # This it will work if only `Spark` is named here, but booting the cluster seems
  # to be much faster when `Hadoop` is included. Ingests, etc., will succeed
  # even if `Hadoop` is missing here.
  applications = ["Hadoop", "Spark", "Zeppelin", "HBase", "Ganglia"]

  ec2_attributes {
    instance_profile = "${var.emr_instance_profile}" # This seems to be the only necessary field.
    key_name         = "${var.key_name}"
    subnet_id        = "${var.analytics_subnet_id}"
  }

  # MASTER group must have an instance_count of 1.
  # `xlarge` seems to be the smallest type they'll allow (large didn't work).
  instance_group {
    bid_price      = "${var.bid_price}"
    instance_count = 1
    instance_role  = "MASTER"
    instance_type  = "m3.xlarge"
    name           = "OsmesaAnalytics-MasterGroup"
  }

  instance_group {
    bid_price      = "${var.bid_price}"
    instance_count = "${var.worker_count}"
    instance_role  = "CORE"
    instance_type  = "m3.xlarge"
    name           = "OsmesaAnalytics-CoreGroup"
  }

  # Location to dump logs
  log_uri = "${var.s3_uri}"

  tags {
    name = "OSMesa Analytics Cluster"
    role = "${var.emr_service_role}"
    env  = "env"
  }

  bootstrap_action {
    path = "s3://${var.bootstrap_bucket}/${var.bootstrap_prefix}/bootstrap-master.sh"
    name = "bootstrap-master"
  }

  depends_on = ["aws_s3_bucket_object.bootstrap-master"]

  configurations  = "${data.template_file.emr_configurations.rendered}"

  # This is the effect of `aws emr create-cluster --use-default-roles`.
  service_role = "${var.emr_service_role}"
}

# Pipable to other programs.
output "emrID" {
  value = "${aws_emr_cluster.emrSparkCluster.id}"
}
