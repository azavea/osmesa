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

.EXPORT_ALL_VARIABLES:

#############################
#  Docker image management  #
#############################

.PHONY: build-container login-aws-registry tag-image push-image

build-container:
	cd .. && VERSION_TAG=${VERSION_TAG} ./build-container.sh

login-aws-registry:
	eval `aws ecr get-login --no-include-email --region ${AWS_REGION}`

tag-image: build-container
	docker tag osm_apps:${VERSION_TAG} ${ECR_IMAGE}:${VERSION_TAG}

push-image: login-aws-registry tag-image
	docker push ${ECR_IMAGE}:${VERSION_TAG}

#######################
# Streaming AWS Tasks #
#######################

.PHONY: create-log-groups define-streaming-vectortile-tasks define-staging-streaming-update-tasks define-production-streaming-update-tasks deploy-streaming-footprint-updater deploy-streaming-edit-histogram-updater deploy-streaming-stats-updaters

create-log-groups:
	./scripts/create-log-groups.sh

define-streaming-vectortile-tasks:
	./scripts/define-streaming-vectortile-tasks.sh

define-staging-streaming-update-tasks:
	./scripts/define-staging-streaming-update-tasks.sh

define-production-streaming-update-tasks:
	./scripts/define-production-streaming-update-tasks.sh

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

deploy-streaming-vectortile-tasks: deploy-footprint-updater deploy-streaming-edit-histogram-updater

###################
# Batch AWS Tasks #
###################

batch-generate-footprints:
	./scripts/batch-generate-footprints.sh

batch-generate-edit-histograms:
	./scripts/batch-generate-edit-histograms.sh

batch-generate-db-backfill:
	./scripts/batch-process.sh "OSMesa Batch Process" "ChangesetStatsCreator" 64 "[\"spark-submit\", \"--deploy-mode\", \"cluster\", \"--class\", \"osmesa.apps.batch.ChangesetStatsCreator\", \"${OSMESA_APPS_JAR}\", \"--history\", \"${HISTORY_ORC}\", \"--changesets\", \"${CHANGESETS_ORC}\", \"--changeset-stream\", \"${CHANGESET_SOURCE}\", \"--database-url\", \"${DB_URI}\"]"
