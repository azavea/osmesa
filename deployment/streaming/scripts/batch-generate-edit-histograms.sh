#!/bin/bash

if [ -z ${VERSION_TAG+x} ]; then
    echo "Do not run this script directly.  Use the Makefile in the parent directory."
    exit 1
fi

aws emr create-cluster \
    --applications Name=Ganglia Name=Spark \
    --ebs-root-volume-size 10 \
    --ec2-attributes '{
	      "KeyName": "${KEYPAIR}",
	      "InstanceProfile":"EMR_EC2_DefaultRole",
	      "ServiceAccessSecurityGroup": "${SERVICE_ACCESS_SECURITY_GROUP}",
	      "SubnetId": "${SUBNET}",
	      "EmrManagedSlaveSecurityGroup": "${EMR_SLAVE_SECURITY_GROUP}",
	      "EmrManagedMasterSecurityGroup": "${EMR_MASTER_SECURITY_GROUP}"
	    }' \
    --service-role EMR_DefaultRole \
    --release-label emr-5.19.0 \
    --name 'Faceted State of the Data tile generation' \
    --instance-groups '[
	      {
	        "InstanceCount": 1,
	        "BidPrice": "OnDemandPrice",
	        "InstanceGroupType": "MASTER",
	        "InstanceType": "${BATCH_INSTANCE_TYPE}",
	        "Name":"Master"
	      }, {
	        "InstanceCount": 20,
	        "BidPrice": "OnDemandPrice",
	        "InstanceGroupType": "CORE",
	        "InstanceType": "${BATCH_INSTANCE_TYPE}",
	        "Name":"Workers"
	      }
	    ]' \
    --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
    --auto-terminate \
    --region us-east-1 \
    --steps '[
	      {
	        "Args": [
	          "spark-submit",
	          "--deploy-mode", "cluster",
	          "--class", "osmesa.analytics.oneoffs.FacetedEditHistogramTileCreator",
	          "--conf", "spark.executor.memoryOverhead=2g",
	          "--conf", "spark.sql.shuffle.partitions=2000",
	          "--conf", "spark.speculation=true",
	          "${OSMESA_ANALYTICS_JAR}",
	          "--history", "${HISTORY_ORC}",
	          "--out", "${HISTOGRAM_VT_LOCATION}"
	        ],
	        "Type": "CUSTOM_JAR",
	        "ActionOnFailure": "TERMINATE_CLUSTER",
	        "Jar": "command-runner.jar",
	        "Properties": "",
	        "Name": "FacetedEditHistogramTileCreator"
	      }
	    ]'
