#
# Autoscaling Resources
#
data "template_file" "container_instance_cloud_config" {
  template = "${file("cloud-config/container-instance.yml.tpl")}"

  vars {
    environment   = "${var.environment}"
  }
}

#
# ECS resources
#
module "container_service_cluster" {
  source = "github.com/azavea/terraform-aws-ecs-cluster?ref=0.8.0"

  lookup_latest_ami = true
  vpc_id            = "${var.vpc_id}"
  instance_type     = "${var.container_instance_type}"
  key_name          = "${var.aws_key_name}"
  cloud_config      = "${data.template_file.container_instance_cloud_config.rendered}"

  health_check_grace_period = "600"
  desired_capacity          = "1"
  min_size                  = "1"
  max_size                  = "1"


  private_subnet_ids = ["${var.private_subnet_ids}"]

  project     = "${var.project}"
  environment = "${var.environment}"
}

