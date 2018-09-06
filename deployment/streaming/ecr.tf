#
# Changeset stream
#
resource "aws_ecs_service" "changeset_stream" {
  name            = "Changeset-Stream"
  cluster         = "${module.container_service_cluster.id}"
  task_definition = "${aws_ecs_task_definition.changeset_stream.arn}"
  desired_count   = 1
  depends_on      = ["module.container_service_cluster"]
}

data "template_file" "changeset_stream_task" {
  template = "${file("task-definitions/changeset_stream.yml.tpl")}"

  vars {
    osm_analytics_container_url = "${aws_ecr_repository.osm_analytics.repository_url}"
    changeset_source = "${var.changeset_source}"
    database_url = "${var.database_url}"
    start_sequence = "${var.start_sequence}"
    log_group_name = "${var.environment}Stream"
    log_group_prefix = "osm"
    log_group_region = "${var.aws_region}"
  }
}

resource "aws_ecs_task_definition" "changeset_stream" {
  family                = "changeset_stream"
  container_definitions = "${data.template_file.changeset_stream_task.rendered}"
}

#
# AugmentedDiff stream
#
resource "aws_ecs_service" "augdiff_stream" {
  name            = "Augmented-Diff-Stream"
  cluster         = "${module.container_service_cluster.id}"
  task_definition = "${aws_ecs_task_definition.augdiff_stream.arn}"
  desired_count   = 1
  depends_on      = ["module.container_service_cluster"]
}

data "template_file" "augdiff_stream_task" {
  template = "${file("task-definitions/augdiff_stream.yml.tpl")}"

  vars {
    osm_analytics_container_url = "${aws_ecr_repository.osm_analytics.repository_url}"
    augdiff_source = "${var.augdiff_source}"
    database_url = "${var.database_url}"
    start_sequence = "${var.start_sequence}"
    log_group_name = "${var.environment}Stream"
    log_group_prefix = "osm"
    log_group_region = "${var.aws_region}"
  }
}

resource "aws_ecs_task_definition" "augdiff_stream" {
  family                = "augdiff_stream"
  container_definitions = "${data.template_file.augdiff_stream_task.rendered}"
}

resource "aws_ecr_repository" "osm_analytics" {
  name = "osm_analytics"
}

#
# ECR url for pushing containers up
#
output "osm_analytics_container_url" {
  value = "${aws_ecr_repository.osm_analytics.repository_url}"
}
