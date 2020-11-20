#!/bin/bash

if [ -z ${VERSION_TAG+x} ]; then
    echo "Do not run this script directly.  Use the Makefile in the parent directory."
    exit 1
fi

aws ecs register-task-definition \
    --family "${AUGDIFF_SERVICE_NAME}" \
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
	          \"awslogs-group\": \"/ecs/osmesa-streaming-augdiff-producer\",
	          \"awslogs-region\": \"${AWS_REGION}\",
	          \"awslogs-stream-prefix\": \"ecs\"
	        }
	      },
	      \"command\": [
	        \"${AUGDIFF_SOURCE}\"
	      ],
	      \"environment\": [
	        {
	          \"name\": \"OVERPASS_URL\",
	          \"value\": \"${OVERPASS_URL}\"
	        },
	        {
	          \"name\": \"ONRAMP_URL\",
	          \"value\": \"${ONRAMP_URL}\"
	        }
	      ],
	      \"image\": \"${AUGDIFF_ECR_IMAGE}\",
	      \"name\": \"${AUGDIFF_SERVICE_NAME}\"
	    }
	  ]"
