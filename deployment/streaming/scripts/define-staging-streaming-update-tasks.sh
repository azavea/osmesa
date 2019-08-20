#!/bin/bash

if [ -z ${AWS_REGION+x} ]; then
    echo "Do not run this script directly.  Use the Makefile in the parent directory."
    exit 1
fi

aws ecs register-task-definition \
    --family streaming-stats-updater-staging \
    --task-role-arn "arn:aws:iam::${IAM_ACCOUNT}:role/ECSTaskS3" \
    --execution-role-arn "arn:aws:iam::${IAM_ACCOUNT}:role/ecsTaskExecutionRole" \
    --network-mode awsvpc \
    --requires-compatibilities EC2 FARGATE \
    --cpu "1 vCPU" \
    --memory "4 GB" \
    --container-definitions "[
	    {
	      \"logConfiguration\": {
	        \"logDriver\": \"awslogs\",
	        \"options\": {
	          \"awslogs-group\": \"/ecs/${AWS_LOG_GROUP}-staging\",
	          \"awslogs-region\": \"${AWS_REGION}\",
	          \"awslogs-stream-prefix\": \"ecs\"
	        }
	      },
	      \"command\": [
	        \"/spark/bin/spark-submit\",
	        \"--driver-memory\", \"2048m\",
	        \"--class\", \"osmesa.analytics.oneoffs.StreamingChangesetStatsUpdater\",
	        \"/opt/osmesa-analytics.jar\",
	        \"--augmented-diff-source\", \"${AUGDIFF_SOURCE}\"
	      ],
	      \"environment\": [
	        {
	          \"name\": \"DATABASE_URL\",
	          \"value\": \"${DB_BASE_URI}/${STAGING_DB}\"
	        }
	      ],
	      \"image\": \"${ECR_IMAGE}:latest\",
	      \"name\": \"streaming-augmented-diffs-stats-updater-staging\"
	    },
	    {
	      \"logConfiguration\": {
	        \"logDriver\": \"awslogs\",
	        \"options\": {
	          \"awslogs-group\": \"/ecs/${AWS_LOG_GROUP}-staging\",
	          \"awslogs-region\": \"${AWS_REGION}\",
	          \"awslogs-stream-prefix\": \"ecs\"
	        }
	      },
	      \"command\": [
	        \"/spark/bin/spark-submit\",
	        \"--driver-memory\", \"2048m\",
	        \"--class\", \"osmesa.analytics.oneoffs.StreamingChangesetMetadataUpdater\",
	        \"/opt/osmesa-analytics.jar\",
	        \"--changeset-source\", \"${CHANGESET_SOURCE}\"
	      ],
	      \"environment\": [
	        {
	          \"name\": \"DATABASE_URL\",
	          \"value\": \"${DB_BASE_URI}/${STAGING_DB}\"
	        }
	      ],
	      \"image\": \"${ECR_IMAGE}:latest\",
	      \"name\": \"streaming-changesets-stats-updater-staging\"
	    }
	  ]"
