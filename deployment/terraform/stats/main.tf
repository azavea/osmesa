
data "template_file" "container_instance_cloud_config" {
  template = "${file("cloud-config/container-instance.yml.tpl")}"

  vars {}
}

module "container_service_cluster" {
  source = "github.com/azavea/terraform-aws-ecs-cluster?ref=0.6.0"

  vpc_id            = "vpc-617f9604"
  ami_id            = "ami-b2df2ca4"
  instance_type     = "t2.xlarge"
  key_name          = "geotrellis-emr"
  cloud_config      = "${data.template_file.container_instance_cloud_config.rendered}"

  root_block_device_type = "gp2"
  root_block_device_size = "10"

  health_check_grace_period = "600"
  desired_capacity          = "1"
  min_size                  = "1"
  max_size                  = "1"

  enabled_metrics = [
    "GroupMinSize",
    "GroupMaxSize",
    "GroupDesiredCapacity",
    "GroupInServiceInstances",
    "GroupPendingInstances",
    "GroupStandbyInstances",
    "GroupTerminatingInstances",
    "GroupTotalInstances",
  ]

  private_subnet_ids = ["subnet-7d2ba926"]

  project     = "OSMesa"
  environment = "Staging"
}

# Template for container definition, allows us to inject environment
data "template_file" "osmesa_stats_task" {
  template = "${file("containers.json")}"

  vars {
    image    = "quay.io/geotrellis/osmesa-query:latest",
    s3bucket = "geotrellis-test",
    s3prefix = "nathan/osm-stats"
  }
}

# Allows resource sharing among multiple containers
resource "aws_ecs_task_definition" "osmesa_stats" {
  family                = "osmesa_stats"
  container_definitions = "${data.template_file.osmesa_stats_task.rendered}"
}

# Defines running an ECS task as a service
resource "aws_ecs_service" "osmesa_stats" {
  name                               = "osmesa_stats"
  cluster                            = "${module.container_service_cluster.id}"
  task_definition                    = "${aws_ecs_task_definition.osmesa_stats.family}:${aws_ecs_task_definition.osmesa_stats.revision}"
  desired_count                      = "${var.desired_stats_instance_count}"
  deployment_minimum_healthy_percent = "0" # allow old services to be torn down
  deployment_maximum_percent         = "100"
}

