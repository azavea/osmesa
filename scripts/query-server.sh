#!/bin/bash

set -ex

DIR="$(dirname "$0")"
source ${DIR}/util/realpath.sh
REPLICA_STATE_FILE=$(realpath "${DIR}/../deployment/terraform/replica/terraform.tfstate")
KEY_PAIR_NAME=$(cat ${REPLICA_STATE_FILE} | jq -r ".modules[].resources[\"aws_emr_cluster.emrSparkCluster\"].primary.attributes[\"ec2_attributes.0.key_name\"]")
KEY_PAIR_FILE="~/.ssh/${KEY_PAIR_NAME}.pem"
MASTER_DNS=$(cat ${REPLICA_STATE_FILE} | jq '.modules[0].resources["aws_emr_cluster.emrSparkCluster"].primary.attributes.master_public_dns')

temp="${MASTER_DNS%\"}"
temp="${temp#\"}"

ssh -t -i ${KEY_PAIR_FILE} hadoop@${temp} 'docker run -d -e PORT=8898 -p 8898:8898 quay.io/geotrellis/osmesa-query:latest'
