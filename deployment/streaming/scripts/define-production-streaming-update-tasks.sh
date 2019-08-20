#!/bin/bash

if [ -z ${AWS_REGION+x} ]; then
    echo "Do not run this script directly.  Use the Makefile in the parent directory."
    exit 1
fi

aws ecs register-task-definition \
    --family streaming-stats-updater-production \
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
	          \"awslogs-group\": \"/ecs/${AWS_LOG_GROUP}\",
	          \"awslogs-region\": \"${AWS_REGION}\",
	          \"awslogs-stream-prefix\": \"ecs\"
	        }
	      },
	      \"command\": [
	        \"/spark/bin/spark-submit\",
	        \"--driver-memory\", \"2048m\",
	        \"--class\", \"osmesa.analytics.oneoffs.AugmentedDiffStreamProcessor\",
	        \"/opt/osmesa-analytics.jar\",
	        \"--augmented-diff-source\", \"${AUGDIFF_SOURCE}\"
	      ],
	      \"environment\": [
	        {
	          \"name\": \"DATABASE_URL\",
	          \"value\": \"${DB_BASE_URI}/${PRODUCTION_DB}\"
	        }
	      ],
	      \"image\": \"${ECR_IMAGE}:production\",
	      \"name\": \"streaming-augmented-diffs-stats-updater\"
	    },
	    {
	      \"logConfiguration\": {
	        \"logDriver\": \"awslogs\",
	        \"options\": {
	          \"awslogs-group\": \"/ecs/${AWS_LOG_GROUP}\",
	          \"awslogs-region\": \"${AWS_REGION}\",
	          \"awslogs-stream-prefix\": \"ecs\"
	        }
	      },
	      \"command\": [
	        \"/spark/bin/spark-submit\",
	        \"--driver-memory\", \"2048m\",
	        \"--class\", \"osmesa.analytics.oneoffs.ChangesetStreamProcessor\",
	        \"/opt/osmesa-analytics.jar\",
	        \"--changeset-source\", \"${CHANGESET_SOURCE}\"
	      ],
	      \"environment\": [
	        {
	          \"name\": \"DATABASE_URL\",
	          \"value\": \"${DB_BASE_URI}/${PRODUCTION_DB}\"
	        }
	      ],
	      \"image\": \"${ECR_IMAGE}:production\",
	      \"name\": \"streaming-changesets-stats-updater\"
	    }
	  ]"
