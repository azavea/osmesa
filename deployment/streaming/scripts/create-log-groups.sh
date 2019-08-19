#!/bin/bash

if [ -z ${AWS_REGION+x} ]; then
    echo "Do not run this script directly.  Use the Makefile in the parent directory."
    exit 1
fi

DEFINED_GROUPS=$(aws logs describe-log-groups | jq '.logGroups[].logGroupName' | sed -e 's/"//g')

if [[ $DEFINED_GROUPS != *"/ecs/${AWS_LOG_GROUP}"* ]]; then
    aws logs create-log-group \
        --log-group-name /ecs/${AWS_LOG_GROUP}
fi

if [[ $DEFINED_GROUPS != *"/ecs/${AWS_LOG_GROUP}-staging"* ]]; then
    aws logs create-log-group \
        --log-group-name /ecs/${AWS_LOG_GROUP}-staging
fi

if [[ $DEFINED_GROUPS != *"/ecs/streaming-user-footprint-tile-updater"* ]]; then
    aws logs create-log-group \
        --log-group-name /ecs/streaming-user-footprint-tile-updater
fi

if [[ $DEFINED_GROUPS != *"/ecs/streaming-edit-histogram-tile-updater"* ]]; then
    aws logs create-log-group \
        --log-group-name /ecs/streaming-edit-histogram-tile-updater
fi