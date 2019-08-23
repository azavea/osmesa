#!/bin/bash

if [ -z ${VERSION_TAG+x} ]; then
    echo "Do not run this script directly.  Use the Makefile in the parent directory."
    exit 1
fi

CLUSTER_NAME=$1
STEP_NAME=$2
NUM_EXECUTORS=$3
ARGS=$4

set -x
aws emr create-cluster \
    --configurations "file://$(pwd)/scripts/configurations.json" \
    --applications Name=Ganglia Name=Spark \
    --log-uri ${S3_LOG_URI} \
    --ebs-root-volume-size 10 \
    --ec2-attributes "{
	      \"KeyName\": \"${KEYPAIR}\",
	      \"InstanceProfile\":\"EMR_EC2_DefaultRole\",
	      \"SubnetId\": \"${SUBNET}\",
	      \"EmrManagedMasterSecurityGroup\": \"${MASTER_SECURITY_GROUP}\",
	      \"EmrManagedSlaveSecurityGroup\": \"${WORKER_SECURITY_GROUP}\",
	      \"ServiceAccessSecurityGroup\": \"${SERVICE_ACCESS_SG}\",
              \"AdditionalMasterSecurityGroups\": [\"${SANDBOX_SG}\"],
              \"AdditionalSlaveSecurityGroups\": [\"${SANDBOX_SG}\"]
	    }" \
    --service-role EMR_DefaultRole \
    --release-label emr-5.19.0 \
    --name "$CLUSTER_NAME" \
    --instance-groups "[
	      {
	        \"InstanceCount\": 1,
	        \"BidPrice\": \"OnDemandPrice\",
	        \"InstanceGroupType\": \"MASTER\",
	        \"InstanceType\": \"${BATCH_INSTANCE_TYPE}\",
	        \"Name\":\"Master\"
	      }, {
	        \"InstanceCount\": ${NUM_EXECUTORS},
	        \"BidPrice\": \"OnDemandPrice\",
	        \"InstanceGroupType\": \"CORE\",
	        \"InstanceType\": \"${BATCH_INSTANCE_TYPE}\",
	        \"Name\":\"Workers\"
	      }
	    ]" \
    --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
    --auto-terminate \
    --region us-east-1 \
    --steps "[
	      {
	        \"Args\": $ARGS,
	        \"Type\": \"CUSTOM_JAR\",
	        \"ActionOnFailure\": \"TERMINATE_CLUSTER\",
	        \"Jar\": \"command-runner.jar\",
	        \"Properties\": \"\",
	        \"Name\": \"$STEP_NAME\"
	      }
	    ]"
