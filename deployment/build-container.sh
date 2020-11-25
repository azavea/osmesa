#!/bin/bash

if [ -z ${VERSION_TAG+x} ]; then
    echo "No version tag has been set.  Do not run this script directly; instead, issue"
    echo "                              make build-container"
    echo "from the 'streaming' directory."
    exit 1
else
    echo "Version tag is set to '${VERSION_TAG}'"
fi

set -xe
SBT_DIR="../src"
JAR_DIR=${SBT_DIR}/apps/target/scala-2.11/
DOCKER_DIR=$(pwd)

cp ${JAR_DIR}/osmesa-apps.jar ${DOCKER_DIR}/osmesa-apps.jar
docker build -f Dockerfile --tag osm_apps:${VERSION_TAG} ${DOCKER_DIR}
rm ${DOCKER_DIR}/osmesa-apps.jar
