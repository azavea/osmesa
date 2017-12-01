
# ECS cluster is only a name that ECS machines may join
resource "aws_ecs_cluster" "osmesa_stats" {

  lifecycle {
    create_before_destroy = true
  }

  name = "OSMESA-STATS"
}

# Template for container definition, allows us to inject environment
data "template_file" "osmesa_stats_task" {
  template = "${file("containers.json")}"

  vars {
    image    = "quay.io/geotrellis/osmesa-query:latest",
    s3bucket = "${var.s3bucket}",
    s3prefix = "${var.s3prefix}"
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
  cluster                            = "${aws_ecs_cluster.osmesa_stats.id}"
  task_definition                    = "${aws_ecs_task_definition.osmesa_stats.family}:${aws_ecs_task_definition.osmesa_stats.revision}"
  desired_count                      = "${var.desired_stats_instance_count}"
  deployment_minimum_healthy_percent = "0" # allow old services to be torn down
  deployment_maximum_percent         = "100"
}

#
# AutoScaling resources
#

# Defines a launch configuration for ECS worker, associates it with our cluster
resource "aws_launch_configuration" "ecs" {
  name = "ECS ${aws_ecs_cluster.osmesa_stats.name}"
  image_id             = "${var.aws_ecs_ami}"
  instance_type        = "${var.ecs_instance_type}"
  iam_instance_profile = "${var.ecs_instance_profile}"

  # TODO: is there a good way to make the key configurable sanely?
  key_name             = "${var.ec2_key}"
  associate_public_ip_address = true
}

# Auto-scaling group for ECS workers
resource "aws_autoscaling_group" "ecs" {
  lifecycle {
    create_before_destroy = true
  }

  # Explicitly linking ASG and launch configuration by name
  # to force replacement on launch configuration changes.
  name = "${aws_launch_configuration.ecs.name}"

  launch_configuration      = "${aws_launch_configuration.ecs.name}"
  health_check_grace_period = 600
  health_check_type         = "EC2"
  desired_capacity          = "${var.desired_stats_instance_count}"
  min_size                  = "${var.desired_stats_instance_count}"
  max_size                  = "${var.desired_stats_instance_count}"
  vpc_zone_identifier       = ["${var.subnet_id}"]

  tag {
    key                 = "Name"
    value               = "ECS ${aws_ecs_cluster.osmesa_stats.name}"
    propagate_at_launch = true
  }
}


