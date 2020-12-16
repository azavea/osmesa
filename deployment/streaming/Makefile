include config-deployment.mk

# If the user is on master branch, see if we should deploy to production
VERSION_TAG:=$(shell ./scripts/get-tag.sh)
ifeq ($(VERSION_TAG), production)
	DATABASE=${PRODUCTION_DB}
	ECS_CLUSTER=${CLUSTER_NAME_DEPLOYMENT}
	TASK_SUFFIX=
else
	DATABASE=${STAGING_DB}
	ECS_CLUSTER=${CLUSTER_NAME_STAGING}
	TASK_SUFFIX=-staging
endif

DB_URI=${DB_BASE_URI}/${DATABASE}

ifndef STATS_SNAPSHOT_ORC
SNAPSHOT_OPT=
BATCH_WORKERS=64
else
ifndef USE_SNAPSHOT
SNAPSHOT_OPT=, \"--stats-checkpoint\", \"${STATS_SNAPSHOT_ORC}\"
BATCH_WORKERS=64
else
SNAPSHOT_OPT=, \"--stats-checkpoint\", \"${STATS_SNAPSHOT_ORC}\", \"--use-checkpoint\"
BATCH_WORKERS=1
endif
endif

ifndef TURBO_BOOST
NODE_OPTIONS=
DRIVER_MEMORY=2048m
ECS_MEMORY=4
AUGDIFF_ECS_MEMORY=2
else
NODE_OPTIONS=--max-old-space-size=7168
DRIVER_MEMORY=8192m
ECS_MEMORY=8
AUGDIFF_ECS_MEMORY=8
endif


.EXPORT_ALL_VARIABLES:

#############################
#  Docker image management  #
#############################

.PHONY: build-container login-aws-registry tag-image push-image

APPS_SRC_DIR := ../../src/apps
APPS_ASSEMBLY := ${APPS_SRC_DIR}/target/scala-2.11/${OSMESA_APPS_JAR}

rwildcard=$(foreach d,$(wildcard $1*),$(call rwildcard,$d/,$2) $(filter $(subst *,%,$2),$d))

${APPS_ASSEMBLY}: $(call rwildcard, ${APPS_SRC_DIR}/src, *.scala) ${APPS_SRC_DIR}/build.sbt
	cd ${APPS_SRC_DIR}/.. && ./sbt "project apps" assembly
	@touch -m ${APPS_ASSEMBLY}

upload-apps-jar: ${APPS_ASSEMBLY}
	@aws s3 cp ${APPS_ASSEMBLY} ${S3_ROOT_URI}/

build-container: ${APPS_ASSEMBLY}
	cd .. && VERSION_TAG=${VERSION_TAG} ./build-container.sh

login-aws-registry:
	eval `aws ecr get-login --no-include-email --region ${AWS_REGION}`

tag-image: build-container
	docker tag osm_apps:${VERSION_TAG} ${ECR_STATS_IMAGE}:${VERSION_TAG}
	docker tag osm_refresh:${VERSION_TAG} ${ECR_REFRESH_IMAGE}:${VERSION_TAG}

push-image: login-aws-registry tag-image
	docker push ${ECR_STATS_IMAGE}:${VERSION_TAG}
	docker push ${ECR_REFRESH_IMAGE}:${VERSION_TAG}

#######################
# Streaming AWS Tasks #
#######################

.PHONY: create-log-groups define-streaming-augdiff-producer-task define-streaming-vectortile-tasks define-staging-streaming-update-tasks define-production-streaming-update-tasks deploy-streaming-footprint-updater deploy-streaming-edit-histogram-updater deploy-streaming-stats-updaters

create-log-groups:
	./scripts/create-log-groups.sh

define-streaming-augdiff-producer-task:
	./scripts/define-streaming-augdiff-producer.sh

define-streaming-vectortile-tasks:
	./scripts/define-streaming-vectortile-tasks.sh

define-staging-streaming-update-tasks:
	./scripts/define-staging-streaming-update-tasks.sh

define-production-streaming-update-tasks:
	./scripts/define-production-streaming-update-tasks.sh

define-production-view-refresher-task:
	./scripts/define-production-view-refresher.sh

define-staging-view-refresher-task:
	./scripts/define-staging-view-refresher.sh

deploy-streaming-augdiff-producer:
	aws ecs create-service \
	  --cluster "${ECS_CLUSTER}" \
	  --service-name "${AUGDIFF_SERVICE_NAME}" \
	  --task-definition "${AUGDIFF_SERVICE_NAME}" \
	  --desired-count 1 \
	  --launch-type FARGATE \
	  --scheduling-strategy REPLICA \
	  --network-configuration ${NETWORK_CONFIGURATION}

stop-streaming-footprint-updater:
	./scripts/stop-streaming-service.sh streaming-user-footprint-tile-updater

deploy-streaming-footprint-updater: stop-streaming-footprint-updater
	aws ecs create-service \
	  --cluster "${CLUSTER_NAME_DEPLOYMENT}" \
	  --service-name "streaming-user-footprint-tile-updater" \
	  --task-definition "streaming-user-footprint-tile-updater" \
	  --desired-count 1 \
	  --launch-type FARGATE \
	  --scheduling-strategy REPLICA \
	  --network-configuration ${NETWORK_CONFIGURATION}

stop-streaming-edit-histogram-updater:
	./scripts/stop-streaming-service.sh streaming-edit-histogram-tile-updater

deploy-streaming-edit-histogram-updater: stop-streaming-edit-histogram-updater
	aws ecs create-service \
	  --cluster "${CLUSTER_NAME_DEPLOYMENT}" \
	  --service-name "streaming-edit-histogram-tile-updater" \
	  --task-definition "streaming-edit-histogram-tile-updater" \
	  --desired-count 1 \
	  --launch-type FARGATE \
	  --scheduling-strategy REPLICA \
	  --network-configuration ${NETWORK_CONFIGURATION}

stop-streaming-stats-updaters:
	./scripts/stop-streaming-service.sh streaming-stats-updater

deploy-streaming-stats-updaters: stop-streaming-stats-updaters
	aws ecs create-service \
	  --cluster "${ECS_CLUSTER}" \
	  --service-name "streaming-stats-updater" \
	  --task-definition "streaming-stats-updater${TASK_SUFFIX}" \
	  --desired-count 1 \
	  --launch-type FARGATE \
	  --scheduling-strategy REPLICA \
	  --network-configuration ${NETWORK_CONFIGURATION}

deploy-streaming-vectortile-tasks: deploy-streaming-footprint-updater deploy-streaming-edit-histogram-updater

deploy-stats-refresher:
	./scripts/deploy-stats-refresher.sh

disable-stats-refresher:
	aws events disable-rule --name "osmesa-stats-view-refresher${TASK_SUFFIX}"

enable-stats-refresher:
	aws events enable-rule --name "osmesa-stats-view-refresher${TASK_SUFFIX}"

###################
# Batch AWS Tasks #
###################

batch-generate-footprints:
	./scripts/batch-generate-footprints.sh

batch-generate-edit-histograms:
	./scripts/batch-generate-edit-histograms.sh

batch-prepare-bulk-data:
	@envsubst '$${PLANET_HISTORY_PBF} $${PLANET_HISTORY_ORC_DIR}' < scripts/latest-history-to-orc.sh > /tmp/latest-history-to-orc.sh
	@aws s3 cp /tmp/latest-history-to-orc.sh ${S3_ROOT_URI}/scripts/

	BATCH_CORE_INSTANCE_TYPE=m4.xlarge BATCH_MASTER_INSTANCE_TYPE=m4.xlarge ./scripts/batch-process.sh \
	  "OSMesa Ingest Prep" 1 \
		"Changeset ORC Generation" \
		"[\"spark-submit\", \"--deploy-mode\", \"client\", \"--class\", \"osmesa.apps.batch.ChangesetMetadataCreator\", \"${S3_ROOT_URI}/${OSMESA_APPS_JAR}\", \"--changesets\", \"${CHANGESET_CSV}\", \"--comments\", \"${CHANGESET_COMMENTS_CSV}\", \"--tags\", \"${CHANGESET_TAGS_CSV}\", \"--users\", \"${USERS_CSV}\", \"${CHANGESETS_ORC}\"]" \
		"Copy script" \
		"[\"aws\", \"s3\", \"cp\", \"${S3_ROOT_URI}/scripts/latest-history-to-orc.sh\", \"/tmp\"]" \
		"Prepare script" \
		"[\"chmod\", \"+x\", \"/tmp/latest-history-to-orc.sh\"]" \
		"History PBF to ORC" \
		"[\"/tmp/latest-history-to-orc.sh\"]"

batch-generate-db-backfill:
	BATCH_CORE_INSTANCE_TYPE=r5.xlarge BATCH_MASTER_INSTANCE_TYPE=m4.xlarge ./scripts/batch-process.sh \
	  "OSMesa Batch Process" ${BATCH_WORKERS} \
		"ChangesetStatsCreator" \
		"[\"spark-submit\", \"--deploy-mode\", \"cluster\", \"--class\", \"osmesa.apps.batch.ChangesetStatsCreator\", \"${S3_ROOT_URI}/${OSMESA_APPS_JAR}\", \"--history\", \"${HISTORY_ORC}\", \"--changesets\", \"${CHANGESETS_ORC}\", \"--changeset-stream\", \"${CHANGESET_SOURCE}\", \"--database-url\", \"${DB_URI}\"${SNAPSHOT_OPT}]"
