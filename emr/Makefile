
ifndef AWS_PROFILE
$(error AWS_PROFILE is not set)
endif

CLUSTER_ID ?= $(shell cd cluster && terraform output | grep emr-id | awk '{print $$NF}')
KEY_NAME ?= $(shell cd cluster && terraform output | grep key-name | awk '{print $$NF}')
KEY_PATH ?= "~/.ssh/${KEY_NAME}.pem"

# For circumvention of MFA when necessary
AWS_ENV_VARS ?= AWS_ACCESS_KEY_ID=$(shell cat auth.json | jq -re '.Credentials.AccessKeyId') AWS_SECRET_ACCESS_KEY=$(shell cat auth.json | jq -re '.Credentials.SecretAccessKey') AWS_SESSION_TOKEN=$(shell cat auth.json | jq -re '.Credentials.SessionToken')

# Get STS token to work around terraform's MFA difficulties
auth.json:
	rm -rf auth.json
	cd cluster; aws \
		--profile ${AWS_PROFILE} \
		sts assume-role \
		--role-arn="$(shell aws configure get --profile ${AWS_PROFILE} role_arn)" \
		--role-session-name="power-user-session" > ../auth.json

validate-cluster: auth.json
	cd cluster; $(AWS_ENV_VARS) terraform validate \
		--var-file="../tfvars" \
		-var "aws_profile=${AWS_PROFILE}"

init-cluster: auth.json
	cd cluster; $(AWS_ENV_VARS) terraform init \
		-var-file="../tfvars" \
		-var "aws_profile=${AWS_PROFILE}"

cluster-tfplan: auth.json
	cd cluster; $(AWS_ENV_VARS) terraform plan \
		-var-file="../tfvars" \
		-var "aws_profile=${AWS_PROFILE}" \
		-out="cluster-tfplan"

cluster: cluster-tfplan
	cd cluster; $(AWS_ENV_VARS) terraform apply "cluster-tfplan"

ssh: auth.json
	$(AWS_ENV_VARS) aws emr ssh \
		--cluster-id ${CLUSTER_ID} \
		--key-pair-file ${KEY_PATH}

destroy-cluster: auth.json
	cd cluster; $(AWS_ENV_VARS) terraform destroy \
		-var-file="../tfvars" \
		-var "aws_profile=${AWS_PROFILE}"

osmesa.jar:
	echo TODO
	# cd ../src && sbt assembly
	# cp ../src/target/scala-2.11/osmesa-assembly-0.1.0.jar osmesa.jar

upload-jar: osmesa.jar
	echo TODO
	# aws emr put --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PATH} \
	# --src osmesa.jar --dest /tmp/osmesa.jar


print-vars:
	echo aws_profile: ${AWS_PROFILE}
	echo cluster_id: ${CLUSTER_ID}
	echo key_name: ${KEY_NAME}
	echo key_path: ${KEY_PATH}
	echo env_vars: ${AWS_ENV_VARS}

clean:
	rm -rf osmesa.jar
