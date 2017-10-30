ifndef CLUSTER_ID
CLUSTER_ID=$(shell cat deploy/terraform.tfstate | jq -r ".modules[].outputs.emrID.value")
endif
ifndef KEY_PAIR_FILE
KEY_PAIR_FILE=${HOME}/geotrellis-emr.pem
endif

ASSEMBLY_PATH := target/scala-2.11/vp-io-test-assembly-1.0.0.jar

create-cluster:
	cd deploy && terraform apply

destroy-cluster:
	cd deploy && terraform destroy

upload-code:
	aws s3 cp ${ASSEMBLY_PATH} s3://vectortiles/jars/vp-io-test-assembly-rde-1.0.0.jar

run-ingest:
	aws emr add-steps --cluster-id ${CLUSTER_ID} --steps file://${PWD}/deploy/steps.json --region us-east-1

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

rm-finland:
	aws s3 rm s3://vectortiles/rde/orc-catalog/_attributes/metadata__finland__14.json

stop-zeppelin:
	aws emr ssh --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE} --region us-east-1 \
	--command 'sudo stop zeppelin'

start-zeppelin:
	aws emr ssh --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE} --region us-east-1 \
	--command 'sudo start zeppelin'

proxy:
	aws emr socks --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE} --region us-east-1
