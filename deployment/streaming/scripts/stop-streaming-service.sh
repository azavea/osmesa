#!/bin/bash

if [ -z ${VERSION_TAG+x} ]; then
    echo "Do not run this script directly.  Use the Makefile in the parent directory."
    exit 1
fi

SERVICE=$1
echo "Attempting to stop $SERVICE on cluster $ECS_CLUSTER"

check_status() {
    STATUS=$(aws ecs describe-services --services $SERVICE --cluster $ECS_CLUSTER | jq '.services[].status')
}

check_status
if [[ $STATUS == "\"ACTIVE\"" ]]; then
    aws ecs delete-service --service $SERVICE --cluster $ECS_CLUSTER --force
    echo "Waiting for shut down"
    check_status
    while [[ $STATUS != "\"INACTIVE\"" ]]; do
        echo "  current status: $STATUS, still waiting"
        sleep 15s
        check_status
    done
    echo "  final status: $STATUS"
else
    echo "Status was $STATUS, nothing to stop"
fi
