[{
  "name": "augdiff_stream",
  "image": "${osm_analytics_container_url}:latest",
  "memory": 4000,
  "cpu": 2,
  "essential": true,
  "command": [
    "/spark/bin/spark-submit",
    "--class", "osmesa.analytics.oneoffs.AugmentedDiffStreamProcessor",
    "/opt/osmesa-analytics.jar",
    "--augmented-diff-source", "${augdiff_source}",
    "--database-url", "${database_url}",
    "--start-sequence", "${start_sequence}"
  ],
  "logConfiguration": {
    "logDriver": "awslogs",
    "options": {
      "awslogs-group": "${log_group_name}",
      "awslogs-region": "${log_group_region}",
      "awslogs-stream-prefix": "${log_group_prefix}"
    }
  }
}]
