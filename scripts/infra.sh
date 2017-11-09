#!/bin/bash

set -e

if [[ -n "${OSMESA_DEBUG}" ]]; then
    set -x
fi

DIR="$(dirname "$0")"

source ${DIR}/util/realpath.sh

function usage() {
    echo -n \
"Usage: $(basename "$0") COMMAND OPTION STACK
Execute Terraform subcommands with remote state management.

OPTIONS:
  plan
  apply
  destroy

STACKS
  --analytics
"
}

if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    if [ "${1:-}" = "--help" ]; then
        usage
    else
        TERRAFORM_DIR="${DIR}/../deployment/terraform/analytics"
    fi

    if [[ -n "${OSMESA_SETTINGS_BUCKET}" ]]; then
        aws s3 cp "s3://${OSMESA_SETTINGS_BUCKET}/terraform/osmesa/terraform.tfvars" \
            "${TERRAFORM_DIR}/${OSMESA_SETTINGS_BUCKET}.tfvars"
        TERRAFORM_SETTINGS=${OSMESA_SETTINGS_BUCKET}.tfvars
    else
        TERRAFORM_SETTINGS=$(realpath ${DIR}/../deployment/terraform/terraform.tfvars)
    fi

    case "${2:-}" in
        "--analytics")
            TERRAFORM_DIR="${DIR}/../deployment/terraform/analytics"
            IS_EPHEMERAL_CLUSTER=1
            echo
            echo "Attempting to deploy analytic stack with version [${GIT_COMMIT}]..."
            echo "-----------------------------------------------------"
            echo

            ;;
        "--replica")
            TERRAFORM_DIR="${DIR}/../deployment/terraform/replica"
            IS_EPHEMERAL_CLUSTER=1
            echo
            echo "Attempting to deploy read-replica stack with version [${GIT_COMMIT}]..."
            echo "-----------------------------------------------------"
            echo

            ;;
        *)
            usage; exit 1 ;;
    esac

    if [[ ! -f $TERRAFORM_SETTINGS ]]; then
        echo "ERROR: Either OSMESA_SETTINGS_DIR must be set or ./deployment/terraform/terraform.tfvars must exist"
        exit 1
    else
        pushd "${TERRAFORM_DIR}"

        if [[ -n "${IS_EPHEMERAL_CLUSTER}" ]]; then
            TF_PLAN_FILE="terraform.tfplan"

            # for Ephemeral clusters, we do not store state on S3.
            case "${1}" in
                plan)
                    terraform init
                    terraform plan \
                              -var-file="${TERRAFORM_SETTINGS}" \
                              -var="git_commit=\"${GIT_COMMIT}\"" \
                              -out="${TF_PLAN_FILE}"
                    ;;
                apply)
                    terraform apply "${TF_PLAN_FILE}"
                    ;;
                destroy)
                    terraform destroy \
                              -var-file="${TERRAFORM_SETTINGS}"
                    ;;
                *)
                    echo "ERROR: I don't have support for that Terraform subcommand!"
                    exit 1
                    ;;
            esac
        else
            TF_PLAN_FILE="${OSMESA_SETTINGS_BUCKET}.tfplan"

            if [ ! -f $TF_PLAN_FILE ]; then
                echo "ERROR: OSMESA_SETTINGS_DIR must be set for deployment of anything besides emphemeral EMR clusters."
                exit 1
            fi

            case "${1}" in
                plan)
                    rm -rf .terraform terraform.tfstate*
                    terraform init \
                              -backend-config="bucket=${OSMESA_SETTINGS_BUCKET}" \
                              -backend-config="key=terraform/state"
                    terraform plan \
                              -var-file="${TERRAFORM_SETTINGS}" \
                              -var="image_version=\"${GIT_COMMIT}\"" \
                              -out="${OSMESA_SETTINGS_BUCKET}.tfplan"
                    ;;
                apply)
                    terraform apply "${OSMESA_SETTINGS_BUCKET}.tfplan"
                    ;;
                *)
                    echo "ERROR: I don't have support for that Terraform subcommand!"
                    exit 1
                    ;;
            esac
        fi

        popd

    fi
fi
