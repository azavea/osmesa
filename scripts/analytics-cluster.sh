#!/bin/bash

set -e

if [[ -n "${OSMESA_DEBUG}" ]]; then
    set -x
fi

DIR="$(dirname "$0")"

source ${DIR}/util/realpath.sh

realpath $DIR

ASSEMBLY_PATH=$(realpath ${DIR}/../src/analytics/target/scala-2.11/osmesa-analytics.jar)
NOTEBOOKS_PATH=$(realpath ${DIR}/../notebooks/zeppelin/)

function usage() {
    echo -n \
"Usage: $(basename "$0") COMMAND OPTION[S]

Interact with the analytics cluster. You must first deploy the analytics cluster with the infra.sh script.

Options:
        shell
        put-zeppelin-code
        upload-code
        run-job
        start-zeppelin
        stop-zeppelin
        proxy
"
}

TF_STATE_FILE=$(realpath "${DIR}/../deployment/terraform/analytics/terraform.tfstate")

if [ ! -f ${TF_STATE_FILE} ]; then
    echo "${TF_STATE_FILE} does not exist; you must deploy the analytics cluster with infra.sh"
    exit 1
fi

CLUSTER_ID=$(cat ${TF_STATE_FILE} | jq -r ".modules[].outputs.emrID.value")
KEY_PAIR_FILE=$(cat ${TF_STATE_FILE} | jq -r ".modules[].resources[\"aws_emr_cluster.emrSparkCluster\"].primary.attributes[\"ec2_attributes.0.key_name\"]")
KEY_PAIR_FILE="${KEY_PAIR_FILE}.pem"

# MASTER_DNS=$(aws emr describe-cluster --output json --cluster-id ${CLUSTER_ID} --region us-east-1 | jq -r ".Cluster.MasterPublicDnsName")

# echo $MASTER_DNS

if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    if [ "${1:-}" = "--help" ]; then
        usage
    else
        case "${1}" in
            shell)
                aws emr ssh --cluster-id ${CLUSTER_ID} \
                    --profile osmesa \
		    --key-pair-file ${KEY_PAIR_FILE} \
		    --region us-east-1

                exit 1 ;;
            put-zeppelin-code)
                zip -d ${ASSEMBLY_PATH} META-INF/*.RSA META-INF/*.DSA META-INF/*.SF || true

                aws emr put --cluster-id ${CLUSTER_ID} \
                    --profile osmesa \
		    --key-pair-file ${KEY_PAIR_FILE} \
		    --region us-east-1 \
	            --src ${ASSEMBLY_PATH} --dest /home/hadoop/osmesa-analytics.jar
                ;;
            upload-code)
                zip -d ${ASSEMBLY_PATH} META-INF/*.RSA META-INF/*.DSA META-INF/*.SF || true

                aws s3 cp ${ASSEMBLY_PATH} s3://vectortiles/orc-emr/osmesa-analytics.jar
                # TODO
                ;;
            run-job)
                aws emr add-steps --cluster-id ${CLUSTER_ID} --steps file://${PWD}/deployment/steps.json --region us-east-1
                ;;
            start-zeppelin)
                aws emr ssh --cluster-id ${CLUSTER_ID} \
                    --profile osmesa \
		    --key-pair-file ${KEY_PAIR_FILE} \
		    --region us-east-1 \
                    --command "sudo start zeppelin"
                ;;
            stop-zeppelin)
                aws emr ssh --cluster-id ${CLUSTER_ID} \
                    --profile osmesa \
		    --key-pair-file ${KEY_PAIR_FILE} \
		    --region us-east-1 \
                    --command "sudo stop zeppelin"
                ;;
            proxy)
                aws emr socks --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE} --region us-east-1
                ;;
            *)
                echo "We don't have command ${1}";
                usage;
                exit 1
                ;;
        esac
    fi
else
    echo "wa?"
fi
