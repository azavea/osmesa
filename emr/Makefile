ifndef CLUSTER_ID
CLUSTER_ID=$(shell cat terraform/terraform.tfstate | jq -r .modules[].resources[\"aws_emr_cluster.emr-spark-cluster\"].primary.id)
endif
ifndef KEY_PAIR_FILE
ifndef TF_VAR_pem_path
KEY_PAIR_FILE=$(shell cat terraform/variables.tf.json | jq -r ".variable.pem_path.default")
else
KEY_PAIR_FILE=${TF_VAR_pem_path}
endif
endif

ifndef TF_VAR_user
export TF_VAR_user=${USER}
endif

AWS_ENV_VARS=AWS_ACCESS_KEY_ID=$(shell cat terraform/auth.json | jq -re '.Credentials.AccessKeyId') AWS_SECRET_ACCESS_KEY=$(shell cat terraform/auth.json | jq -re '.Credentials.SecretAccessKey') AWS_SESSION_TOKEN=$(shell cat terraform/auth.json | jq -re '.Credentials.SessionToken')

auth:
	cd terraform; aws \
		--profile urchn \
		sts assume-role \
		--role-arn="$(shell aws configure get --profile urchn role_arn)" \
		--role-session-name="power-user-session" > auth.json

terraform-init:
	cd terraform; terraform init
	cd terraform; $(AWS_ENV_VARS) terraform plan

create-cluster:
	cd terraform; $(AWS_ENV_VARS) terraform apply

create-jupyter-cluster:
	cd terraform ; $(AWS_ENV_VARS) TF_VAR_install_jupyter="true" terraform apply

destroy-cluster:
	cd terraform; $(AWS_ENV_VARS) terraform destroy

proxy:
	cd terraform; aws emr socks --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE}

ssh:
	cd terraform; aws emr ssh --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE}

restart-zeppelin:
	cd terraform; aws emr ssh --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE} \
	--command 'sudo restart zeppelin'

stop-zeppelin:
	cd terraform; aws emr ssh --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE} \
	--command 'sudo stop zeppelin'

start-zeppelin:
	cd terraform; aws emr ssh --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE} \
	--command 'sudo start zeppelin'

