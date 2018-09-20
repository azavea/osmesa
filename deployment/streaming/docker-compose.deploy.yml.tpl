version: '3.0'
services:
  augdiff-stream:
    image: ${ECR_REPO}:latest
    command: >
      /spark/bin/spark-submit --driver-memory 4096m --class osmesa.analytics.oneoffs.AugmentedDiffStreamProcessor /opt/osmesa-analytics.jar
      --augmented-diff-source ${AUGDIFF_SOURCE}
      --start-sequence ${AUGDIFF_START}
      --database-uri ${DB_URI}
    deploy:
      restart_policy:
        condition: on-failure
        delay: 1s
        max_attempts: 10
        window: 120s
    logging:
      driver: awslogs
      options:
        awslogs-group: ${AWS_LOG_GROUP}
        awslogs-region: ${AWS_REGION}
        awslogs-stream-prefix: augdiff
  changeset-stream:
    image: ${ECR_REPO}:latest
    command: >
      /spark/bin/spark-submit --driver-memory 4096m --class osmesa.analytics.oneoffs.ChangesetStreamProcessor /opt/osmesa-analytics.jar
      --changeset-source ${CHANGESET_SOURCE}
      --start-sequence ${CHANGESET_START}
      --database-uri ${DB_URI}
    deploy:
      restart_policy:
        condition: on-failure
        delay: 1s
        max_attempts: 10
        window: 120s
    logging:
      driver: awslogs
      options:
        awslogs-group: ${AWS_LOG_GROUP}
        awslogs-region: ${AWS_REGION}
        awslogs-stream-prefix: changeset
