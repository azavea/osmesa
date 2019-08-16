include config-deployment.mk

# If the user is on master branch, see if we should deploy to production
VERSION_TAG=$(shell ./get-tag.sh)
ifeq ($(VERSION_TAG), production)
	DATABASE=${PRODUCTION_DB}
	ECS_CLUSTER=osm-stat-stream-cluster
	TASK_SUFFIX=
else
	DATABASE=${STAGING_DB}
	ECS_CLUSTER=osm-stats-staging
	TASK_SUFFIX="-staging"
endif
DB_URI=${DB_BASE_URI}${DATABASE}

#############################
#  Docker image management  #
#############################

.PHONY: build-container login-aws-registry tag-image push-image

build-container:
	cd .. && VERSION_TAG=${VERSION_TAG} ./build-container.sh

login-aws-registry:
	eval `aws ecr get-login --no-include-email --region ${AWS_REGION}`

tag-image: build-container
	docker tag osm_analytics:${VERSION_TAG} ${ECR_IMAGE}:${VERSION_TAG}

push-image: login-aws-registry tag-image
	docker push ${ECR_IMAGE}${VERSION_TAG}

#######################
# Streaming AWS Tasks #
#######################

.PHONY: define-tasks configure-cluster

create-log-groups:
	aws logs create-log-group \
	  --log-group-name /ecs/${AWS_LOG_GROUP}
	aws logs create-log-group \
	  --log-group-name /ecs/${AWS_LOG_GROUP}-staging
	aws logs create-log-group \
	  --log-group-name /ecs/streaming-user-footprint-tile-updater
	aws logs create-log-group \
	  --log-group-name /ecs/streaming-edit-histogram-tile-updater

define-streaming-vectortile-tasks:
	aws ecs register-task-definition \
	  --family osmesa-streaming-edit-histogram-tile-updater \
	  --task-role-arn "arn:aws:iam::<account>:role/ECSTaskS3" \
	  --execution-role-arn "arn:aws:iam::<account>:role/ecsTaskExecutionRole" \
	  --network-mode awsvpc \
	  --requires-compatibilities EC2 FARGATE \
	  --cpu "1 vCPU" \
	  --memory "2 GB" \
	  --container-definitions '[
	    {
	      "logConfiguration": {
	        "logDriver": "awslogs",
	        "options": {
	          "awslogs-group": "/ecs/streaming-edit-histogram-tile-updater",
	          "awslogs-region": "${AWS_REGION}",
	          "awslogs-stream-prefix": "ecs"
	        }
	      },
	      "command": [
	        "/spark/bin/spark-submit",
	        "--driver-memory", "2048m",
	        "--class", "osmesa.analytics.oneoffs.StreamingFacetedEditHistogramTileUpdater",
	        "/opt/osmesa-analytics.jar",
	        "--augmented-diff-source", "${AUGDIFF_SOURCE}",
	        "--tile-source", "${HISTOGRAM_VT_LOCATION}"
	      ],
	      "environment": [
	        {
	          "name": "DATABASE_URL",
	          "value": "${DB_BASE_URI}${PRODUCTION_DB}"
	        }
	      ],
	      "image": "${ECR_IMAGE}",
	      "name": "streaming-edit-histogram-tile-updater"
	    },
	    {
	      "logConfiguration": {
	        "logDriver": "awslogs",
	        "options": {
	          "awslogs-group": "/ecs/streaming-user-footprint-tile-updater",
	          "awslogs-region": "${AWS_REGION}",
	          "awslogs-stream-prefix": "ecs"
	        }
	      },
	      "command": [
	        "/spark/bin/spark-submit",
	        "--driver-memory", "2048m",
	        "--class", "osmesa.analytics.oneoffs.StreamingUserFootprintTileUpdater",
	        "/opt/osmesa-analytics.jar",
	        "--change-source", "${CHANGESET_SOURCE}",
	        "--tile-source", "${FOOTPRINT_VT_LOCATION}"
	      ],
	      "environment": [
	        {
	          "name": "DATABASE_URL",
	          "value": "${DB_BASE_URI}${PRODUCTION_DB}"
	        }
	      ],
	      "image": "${ECR_IMAGE}:production",
	      "name": "streaming-user-footprint-tile-updater"
	    }
	  ]'

define-staging-streaming-update-tasks:
	aws ecs register-task-definition \
	  --family osmesa-streaming-stats-updater \
	  --task-role-arn "arn:aws:iam::<account>:role/ECSTaskS3" \
	  --execution-role-arn "arn:aws:iam::<account>:role/ecsTaskExecutionRole" \
	  --network-mode awsvpc \
	  --requires-compatibilities EC2 FARGATE \
	  --cpu "1 vCPU" \
	  --memory "4 GB" \
	  --container-definitions '[
	    {
	      "logConfiguration": {
	        "logDriver": "awslogs",
	        "options": {
	          "awslogs-group": "/ecs/${AWS_LOG_GROUP}-staging",
	          "awslogs-region": "${AWS_REGION}",
	          "awslogs-stream-prefix": "ecs"
	        }
	      },
	      "command": [
	        "/spark/bin/spark-submit",
	        "--driver-memory", "2048m",
	        "--class", "osmesa.analytics.oneoffs.AugmentedDiffStreamProcessor",
	        "/opt/osmesa-analytics.jar",
	        "--augmented-diff-source", "${AUGDIFF_SOURCE}"
	      ],
	      "environment": [
	        {
	          "name": "DATABASE_URL",
	          "value": "${DB_BASE_URI}${STAGING_DB}"
	        }
	      ],
	      "image": "${ECR_IMAGE}:latest",
	      "name": "streaming-augmented-diffs-stats-updater-staging"
	    },
	    {
	      "logConfiguration": {
	        "logDriver": "awslogs",
	        "options": {
	          "awslogs-group": "/ecs/${AWS_LOG_GROUP}-staging",
	          "awslogs-region": "${AWS_REGION}",
	          "awslogs-stream-prefix": "ecs"
	        }
	      },
	      "command": [
	        "/spark/bin/spark-submit",
	        "--driver-memory", "2048m",
	        "--class", "osmesa.analytics.oneoffs.ChangesetStreamProcessor",
	        "/opt/osmesa-analytics.jar",
	        "--changeset-source", "${CHANGESET_SOURCE}"
	      ],
	      "environment": [
	        {
	          "name": "DATABASE_URL",
	          "value": "${DB_URI}${STAGING_DB}"
	        }
	      ],
	      "image": "${ECR_IMAGE}:latest",
	      "name": "streaming-changesets-stats-updater-staging"
	    }
	  ]'

define-production-streaming-update-tasks:
	aws ecs register-task-definition \
	  --family osmesa-streaming-stats-updater \
	  --task-role-arn "arn:aws:iam::<account>:role/ECSTaskS3" \
	  --execution-role-arn "arn:aws:iam::<account>:role/ecsTaskExecutionRole" \
	  --network-mode awsvpc \
	  --requires-compatibilities EC2 FARGATE \
	  --cpu "1 vCPU" \
	  --memory "4 GB" \
	  --container-definitions '[
	    {
	      "logConfiguration": {
	        "logDriver": "awslogs",
	        "options": {
	          "awslogs-group": "/ecs/${AWS_LOG_GROUP}",
	          "awslogs-region": "${AWS_REGION}",
	          "awslogs-stream-prefix": "ecs"
	        }
	      },
	      "command": [
	        "/spark/bin/spark-submit",
	        "--driver-memory", "2048m",
	        "--class", "osmesa.analytics.oneoffs.AugmentedDiffStreamProcessor",
	        "/opt/osmesa-analytics.jar",
	        "--augmented-diff-source", "${AUGDIFF_SOURCE}"
	      ],
	      "environment": [
	        {
	          "name": "DATABASE_URL",
	          "value": "${DB_URI}${PRODUCTION_DB}"
	        }
	      ],
	      "image": "${ECR_IMAGE}:production",
	      "name": "streaming-augmented-diffs-stats-updater"
	    },
	    {
	      "logConfiguration": {
	        "logDriver": "awslogs",
	        "options": {
	          "awslogs-group": "/ecs/${AWS_LOG_GROUP}",
	          "awslogs-region": "${AWS_REGION}",
	          "awslogs-stream-prefix": "ecs"
	        }
	      },
	      "command": [
	        "/spark/bin/spark-submit",
	        "--driver-memory", "2048m",
	        "--class", "osmesa.analytics.oneoffs.ChangesetStreamProcessor",
	        "/opt/osmesa-analytics.jar",
	        "--changeset-source", "${CHANGESET_SOURCE}"
	      ],
	      "environment": [
	        {
	          "name": "DATABASE_URL",
	          "value": "${DB_URI}${PRODUCTION_DB}"
	        }
	      ],
	      "image": "${ECR_IMAGE}:production",
	      "name": "streaming-changesets-stats-updater"
	    }
	  ]'

deploy-streaming-footprint-updater:
	aws ecs create-service \
	  --cluster "osm-stat-stream-cluster" \
	  --service-name "streaming-user-footprint-tile-updater" \
	  --task-definition "streaming-edit-histogram-tile-updater" \
	  --desired-count 1 \
	  --launch-type FARGATE \
	  --scheduling-strategy REPLICA \
	  --network-configuration '{
	    "awsvpcConfiguration": {
	      "subnets": ["${ECS_SUBNET}"],
	      "securityGroups": ["${ECS_SECURITY_GROUP}"],
	      "assignPublicIp": "DISABLED"
	    }
	  }'

deploy-streaming-edit-histogram-updater:
	aws ecs create-service \
	  --cluster "osm-stat-stream-cluster" \
	  --service-name "streaming-edit-histogram-tile-updater" \
	  --task-definition "streaming-edit-histogram-tile-updater" \
	  --desired-count 1 \
	  --launch-type FARGATE \
	  --scheduling-strategy REPLICA \
	  --network-configuration '{
	    "awsvpcConfiguration": {
	      "subnets": ["${ECS_SUBNET}"],
	      "securityGroups": ["${ECS_SECURITY_GROUP}"],
	      "assignPublicIp": "DISABLED"
	    }
	  }'

deploy-streaming-stats-updaters:
	aws ecs create-service \
	  --cluster "${ECS_CLUSTER}" \
	  --service-name "streaming-augdiffs-stats-updater" \
	  --task-definition "streaming-augmented-diffs-stats-updater${TASK_SUFFIX}" \
	  --desired-count 1 \
	  --launch-type FARGATE \
	  --scheduling-strategy REPLICA \
	  --network-configuration '{
	    "awsvpcConfiguration": {
	      "subnets": ["${ECS_SUBNET}"],
	      "securityGroups": ["${ECS_SECURITY_GROUP}"],
	      "assignPublicIp": "DISABLED"
	    }
	  }'
	aws ecs create-service \
	  --cluster "${ECS_CLUSTER}" \
	  --service-name "streaming-changesets-stats-updater" \
	  --task-definition "streaming-changesets-stats-updater${TASK_SUFFIX}" \
	  --desired-count 1 \
	  --launch-type FARGATE \
	  --scheduling-strategy REPLICA \
	  --network-configuration '{
	    "awsvpcConfiguration": {
	      "subnets": ["${ECS_SUBNET}"],
	      "securityGroups": ["${ECS_SECURITY_GROUP}"],
	      "assignPublicIp": "DISABLED"
	    }
	  }'

deploy-all-streaming-tasks: deploy-footprint-updater deploy-streaming-edit-histogram-updater deploy-streaming-stats-updaters

###################
# Batch AWS Tasks #
###################

batch-generate-footprints:
	aws emr create-cluster \
	  --applications Name=Ganglia Name=Spark \
	  --ebs-root-volume-size 10 \
	  --ec2-attributes '{
	      "KeyName": "${KEYPAIR}",
	      "InstanceProfile":"EMR_EC2_DefaultRole",
	      "ServiceAccessSecurityGroup": "${SERVICE_ACCESS_SECURITY_GROUP}",
	      "SubnetId": "${SUBNET}",
	      "EmrManagedSlaveSecurityGroup": "${EMR_SLAVE_SECURITY_GROUP}",
	      "EmrManagedMasterSecurityGroup": "${EMR_MASTER_SECURITY_GROUP}"
	    }' \
	    --service-role EMR_DefaultRole \
	    --release-label emr-5.19.0 \
	    --name 'User footprint tile generation' \
	    --instance-groups '[
	      {
	        "InstanceCount": 1,
	        "BidPrice": "OnDemandPrice",
	        "InstanceGroupType": "MASTER",
	        "InstanceType": "${BATCH_INSTANCE_TYPE}",
	        "Name":"Master"
	      }, {
	        "InstanceCount": 20,
	        "BidPrice": "OnDemandPrice",
	        "InstanceGroupType": "CORE",
	        "InstanceType": "${BATCH_INSTANCE_TYPE}",
	        "Name":"Workers"
	      }
	    ]' \
	    --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
	    --auto-terminate \
	    --region ${AWS_REGION} \
	    --steps '[
	      {
	        "Args": [
	          "spark-submit",
	          "--deploy-mode", "cluster",
	          "--class", "osmesa.analytics.oneoffs.FootprintCommand",
	          "--conf", "spark.executor.memoryOverhead=2g",
	          "--conf", "spark.sql.shuffle.partitions=2000",
	          "--conf", "spark.speculation=true",
	          "${OSMESA_ANALYTICS_JAR}",
	          "--history", "${NOME_HISTORY_ORC}",
	          "--changesets", "${NOME_CHANGESETS_ORC",
	          "--out", "${FOOTPRINT_VT_LOCATION}",
	          "--type", "users",
	        ],
	        "Type": "CUSTOM_JAR",
	        "ActionOnFailure": "TERMINATE_CLUSTER",
	        "Jar": "command-runner.jar",
	        "Properties": "",
	        "Name": "FootprintCommand"
	      }
	    ]'

batch-generate-edit-histograms:
	aws emr create-cluster \
	  --applications Name=Ganglia Name=Spark \
	  --ebs-root-volume-size 10 \
	  --ec2-attributes '{
	      "KeyName": "${KEYPAIR}",
	      "InstanceProfile":"EMR_EC2_DefaultRole",
	      "ServiceAccessSecurityGroup": "${SERVICE_ACCESS_SECURITY_GROUP}",
	      "SubnetId": "${SUBNET}",
	      "EmrManagedSlaveSecurityGroup": "${EMR_SLAVE_SECURITY_GROUP}",
	      "EmrManagedMasterSecurityGroup": "${EMR_MASTER_SECURITY_GROUP}"
	    }' \
	    --service-role EMR_DefaultRole \
	    --release-label emr-5.19.0 \
	    --name 'Faceted State of the Data tile generation' \
	    --instance-groups '[
	      {
	        "InstanceCount": 1,
	        "BidPrice": "OnDemandPrice",
	        "InstanceGroupType": "MASTER",
	        "InstanceType": "${BATCH_INSTANCE_TYPE}",
	        "Name":"Master"
	      }, {
	        "InstanceCount": 20,
	        "BidPrice": "OnDemandPrice",
	        "InstanceGroupType": "CORE",
	        "InstanceType": "${BATCH_INSTANCE_TYPE}",
	        "Name":"Workers"
	      }
	    ]' \
	    --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
	    --auto-terminate \
	    --region us-east-1 \
	    --steps '[
	      {
	        "Args": [
	          "spark-submit",
	          "--deploy-mode", "cluster",
	          "--class", "osmesa.analytics.oneoffs.FacetedEditHistogramTileCreator",
	          "--conf", "spark.executor.memoryOverhead=2g",
	          "--conf", "spark.sql.shuffle.partitions=2000",
	          "--conf", "spark.speculation=true",
	          "${OSMESA_ANALYTICS_JAR}",
	          "--history", "${NOME_HISTORY_ORC}",
	          "--out", "${HISTOGRAM_VT_LOCATION}"
	        ],
	        "Type": "CUSTOM_JAR",
	        "ActionOnFailure": "TERMINATE_CLUSTER",
	        "Jar": "command-runner.jar",
	        "Properties": "",
	        "Name": "FacetedEditHistogramTileCreator"
	      }
	    ]'
