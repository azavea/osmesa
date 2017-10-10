ifndef CLUSTER_ID
CLUSTER_ID=$(shell cat deploy/terraform.tfstate | jq -r ".modules[].outputs.emrID.value")
endif
ifndef KEY_PAIR_FILE
KEY_PAIR_FILE=${HOME}/geotrellis-emr.pem
endif

S3_PATH=s3://osmesa

ASSEMBLY_PATH := ingest/target/scala-2.11/osmesa-ingest.jar

create-cluster:
	cd deploy && terraform apply

destroy-cluster:
	cd deploy && terraform destroy

upload-code:
	aws s3 cp ${ASSEMBLY_PATH} ${S3_PATH}/jars/vp-io-test-assembly-rde-1.0.0.jar

run-ingest:
	aws emr add-steps --cluster-id ${CLUSTER_ID} --steps file://${PWD}/ingest/steps.json --region us-east-1

put-jar:
	aws emr put --cluster-id ${CLUSTER_ID} \
		    --key-pair-file ${KEY_PAIR_FILE} \
		    --region us-east-1 \
	            --src ${ASSEMBLY_PATH} --dest /tmp/vp.jar

get-json:
	aws emr get --cluster-id ${CLUSTER_ID} \
		    --key-pair-file ${KEY_PAIR_FILE} \
		    --region us-east-1 \
	            --src /tmp/geoout.json --dest ${PWD}/geoout.json

stop-zeppelin:
	aws emr ssh --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE} --region us-east-1 \
	--command 'sudo stop zeppelin'

start-zeppelin:
	aws emr ssh --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE} --region us-east-1 \
	--command 'sudo start zeppelin'

proxy:
	aws emr socks --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE} --region us-east-1
