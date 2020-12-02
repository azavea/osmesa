#!/bin/bash

if [ -z ${VERSION_TAG+x} ]; then
    echo "Do not run this script directly.  Use the Makefile in the parent directory."
    exit 1
fi

aws ecs register-task-definition \
    --family osmesa-stats-view-refresher \
    --task-role-arn "arn:aws:iam::${IAM_ACCOUNT}:role/ECSTaskS3" \
    --execution-role-arn "arn:aws:iam::${IAM_ACCOUNT}:role/ecsTaskExecutionRole" \
    --network-mode awsvpc \
    --requires-compatibilities EC2 FARGATE \
    --cpu "0.25 vCPU" \
    --memory "0.5 GB" \
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
                \"refresh-views.sh\"
              ],
              \"environment\": [
                {
                  \"name\": \"DATABASE_URL\",
	          \"value\": \"${DB_BASE_URI}/${PRODUCTION_DB}\"
                },
                {
                  \"name\": \"DATABASE_NAME\",
	          \"value\": \"${PRODUCTION_DB}\"
                }
              ],
	      \"image\": \"${ECR_IMAGE}:production\",
              \"name\": \"stats-view-refresher\"
            }
          ]"
