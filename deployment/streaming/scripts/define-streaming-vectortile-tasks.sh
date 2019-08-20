#!/bin/bash

if [ -z ${AWS_REGION+x} ]; then
    echo "Do not run this script directly.  Use the Makefile in the parent directory."
    exit 1
fi

aws ecs register-task-definition \
    --family streaming-edit-histogram-tile-updater \
    --task-role-arn "arn:aws:iam::${IAM_ACCOUNT}:role/ECSTaskS3" \
    --execution-role-arn "arn:aws:iam::${IAM_ACCOUNT}:role/ecsTaskExecutionRole" \
    --network-mode awsvpc \
    --requires-compatibilities EC2 FARGATE \
    --cpu "1 vCPU" \
    --memory "2 GB" \
    --container-definitions "[
	    {
	      \"logConfiguration\": {
	        \"logDriver\": \"awslogs\",
	        \"options\": {
	          \"awslogs-group\": \"/ecs/streaming-edit-histogram-tile-updater\",
	          \"awslogs-region\": \"${AWS_REGION}\",
	          \"awslogs-stream-prefix\": \"ecs\"
	        }
	      },
	      \"command\": [
	        \"/spark/bin/spark-submit\",
	        \"--driver-memory\", \"2048m\",
	        \"--class\", \"osmesa.analytics.oneoffs.StreamingFacetedEditHistogramTileUpdater\",
	        \"/opt/osmesa-analytics.jar\",
	        \"--augmented-diff-source\", \"${AUGDIFF_SOURCE}\",
	        \"--tile-source\", \"${HISTOGRAM_VT_LOCATION}\"
	      ],
	      \"environment\": [
	        {
	          \"name\": \"DATABASE_URL\",
	          \"value\": \"${DB_BASE_URI}${PRODUCTION_DB}\"
	        }
	      ],
	      \"image\": \"${ECR_IMAGE}\",
	      \"name\": \"streaming-edit-histogram-tile-updater\"
	    },
	    {
	      \"logConfiguration\": {
	        \"logDriver\": \"awslogs\",
	        \"options\": {
	          \"awslogs-group\": \"/ecs/streaming-user-footprint-tile-updater\",
	          \"awslogs-region\": \"${AWS_REGION}\",
	          \"awslogs-stream-prefix\": \"ecs\"
	        }
	      },
	      \"command\": [
	        \"/spark/bin/spark-submit\",
	        \"--driver-memory\", \"2048m\",
	        \"--class\", \"osmesa.analytics.oneoffs.StreamingUserFootprintTileUpdater\",
	        \"/opt/osmesa-analytics.jar\",
	        \"--change-source\", \"${CHANGESET_SOURCE}\",
	        \"--tile-source\", \"${FOOTPRINT_VT_LOCATION}\"
	      ],
	      \"environment\": [
	        {
	          \"name\": \"DATABASE_URL\",
	          \"value\": \"${DB_BASE_URI}${PRODUCTION_DB}\"
	        }
	      ],
	      \"image\": \"${ECR_IMAGE}:production\",
	      \"name\": \"streaming-user-footprint-tile-updater\"
	    }
	  ]"
