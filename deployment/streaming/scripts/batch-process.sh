#!/bin/bash

if [ -z ${VERSION_TAG+x} ]; then
  echo "Do not run this script directly.  Use the Makefile in the parent directory."
  exit 1
fi

CLUSTER_NAME=$1
NUM_EXECUTORS=$2

shift 2

ARGS=
while [ "$#" -gt 1 ] ; do
    ARGS="$ARGS
      {
        \"Args\": $2,
        \"Type\": \"CUSTOM_JAR\",
        \"ActionOnFailure\": \"CONTINUE\",
        \"Jar\": \"command-runner.jar\",
        \"Properties\": \"\",
        \"Name\": \"$1\"
      }"
    if [ "$#" -gt 2 ]; then
        ARGS="$ARGS,"
    fi
    shift 2
done

set -x
aws emr create-cluster \
  --applications Name=Ganglia Name=Spark Name=Hive \
  --log-uri ${S3_LOG_URI} \
  --configurations "file://scripts/emr-configurations/batch-process.json" \
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
  --release-label emr-5.29.0 \
  --name "$CLUSTER_NAME" \
  --instance-groups "[
      {
        \"InstanceCount\": 1,
        \"BidPrice\": \"OnDemandPrice\",
        \"InstanceGroupType\": \"MASTER\",
        \"InstanceType\": \"${BATCH_MASTER_INSTANCE_TYPE}\",
        \"Name\":\"Master\",
        \"EbsConfiguration\": {
          \"EbsOptimized\": true,
          \"EbsBlockDeviceConfigs\": [{
            \"VolumeSpecification\": {
              \"VolumeType\": \"gp2\",
              \"SizeInGB\": 1024
            }
          }]
        }
      }, {
        \"InstanceCount\": ${NUM_EXECUTORS},
        \"BidPrice\": \"OnDemandPrice\",
        \"InstanceGroupType\": \"CORE\",
        \"InstanceType\": \"${BATCH_CORE_INSTANCE_TYPE}\",
        \"Name\":\"Workers\",
        \"EbsConfiguration\": {
          \"EbsOptimized\": true,
          \"EbsBlockDeviceConfigs\": [{
            \"VolumeSpecification\": {
              \"VolumeType\": \"gp2\",
              \"SizeInGB\": 1024
            }
          }]
        }
      }
    ]" \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --auto-terminate \
  --region us-east-1 \
  --steps "[
        $ARGS
    ]"
