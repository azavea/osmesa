#!/bin/bash

if [ -z ${VERSION_TAG+x} ]; then
    echo "Do not run this script directly.  Use the Makefile in the parent directory."
    exit 1
fi

aws events put-rule --schedule-expression "rate(1 minute)" --name osmesa-stats-view-refresher${TASK_SUFFIX}
aws events put-targets \
    --rule "osmesa-stats-view-refresher${TASK_SUFFIX}" \
    --targets "[
      {
      \"Id\": \"osmesa-stats-view-refresher${TASK_SUFFIX}\",
      \"Arn\": \"arn:aws:ecs:${AWS_REGION}:${IAM_ACCOUNT}:cluster/${ECS_CLUSTER}\",
      \"RoleArn\": \"arn:aws:iam::${IAM_ACCOUNT}:role/ecsEventsRole\",
      \"EcsParameters\": {
        \"TaskDefinitionArn\": \"arn:aws:ecs:${AWS_REGION}:${IAM_ACCOUNT}:task-definition/osmesa-stats-view-refresher${TASK_SUFFIX}\",
        \"TaskCount\": 1,
        \"LaunchType\": \"FARGATE\",
        \"NetworkConfiguration\": {
          \"awsvpcConfiguration\": {
            \"Subnets\": [\"${ECS_SUBNET}\"],
            \"SecurityGroups\": [\"${ECS_SECURITY_GROUP}\"],
            \"AssignPublicIp\": \"DISABLED\"
          }
        }
      }
    }
  ]"
