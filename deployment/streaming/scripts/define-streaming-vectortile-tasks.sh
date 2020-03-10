#!/bin/bash

if [ -z ${VERSION_TAG+x} ]; then
    echo "Do not run this script directly.  Use the Makefile in the parent directory."
    exit 1
fi

aws ecs register-task-definition \
    --family streaming-edit-histogram-tile-updater \
    --task-role-arn "arn:aws:iam::${IAM_ACCOUNT}:role/ECSTaskS3" \
    --execution-role-arn "arn:aws:iam::${IAM_ACCOUNT}:role/ecsTaskExecutionRole" \
    --network-mode awsvpc \
    --requires-compatibilities EC2 FARGATE \
    --cpu "4 vCPU" \
    --memory "30 GB" \
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
	        \"--driver-memory\", \"27G\",
	        \"--class\", \"osmesa.apps.streaming.StreamingFacetedEditHistogramTileUpdater\",
	        \"/opt/osmesa-apps.jar\",
	        \"--augmented-diff-source\", \"${AUGDIFF_SOURCE}\",
	        \"--tile-source\", \"${HISTOGRAM_VT_LOCATION}\",
	        \"--batch-size\", \"4\"
	      ],
	      \"environment\": [
	        {
	          \"name\": \"DATABASE_URL\",
	          \"value\": \"${DB_BASE_URI}/${PRODUCTION_DB}\"
	        }
	      ],
	      \"image\": \"${ECR_IMAGE}:production\",
	      \"name\": \"streaming-edit-histogram-tile-updater\"
	    }
	  ]"

aws ecs register-task-definition \
    --family streaming-user-footprint-tile-updater \
    --task-role-arn "arn:aws:iam::${IAM_ACCOUNT}:role/ECSTaskS3" \
    --execution-role-arn "arn:aws:iam::${IAM_ACCOUNT}:role/ecsTaskExecutionRole" \
    --network-mode awsvpc \
    --requires-compatibilities EC2 FARGATE \
    --cpu "2 vCPU" \
    --memory "8 GB" \
    --container-definitions "[
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
	        \"--driver-memory\", \"7G\",
	        \"--class\", \"osmesa.apps.streaming.StreamingUserFootprintTileUpdater\",
	        \"/opt/osmesa-apps.jar\",
	        \"--change-source\", \"${CHANGE_SOURCE}\",
	        \"--tile-source\", \"${FOOTPRINT_VT_LOCATION}\",
	        \"--batch-size\", \"4\"
	      ],
	      \"environment\": [
	        {
	          \"name\": \"DATABASE_URL\",
	          \"value\": \"${DB_BASE_URI}/${PRODUCTION_DB}\"
	        }
	      ],
	      \"image\": \"${ECR_IMAGE}:production\",
	      \"name\": \"streaming-user-footprint-tile-updater\"
	    }
	  ]"
