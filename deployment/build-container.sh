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

cd ${SBT_DIR}
./sbt clean "project apps" assembly
cp ${JAR_DIR}/osmesa-apps.jar ${DOCKER_DIR}/osmesa-apps.jar

cd ${DOCKER_DIR}

docker build -f ${DOCKER_DIR}/Dockerfile --tag osm_apps:${VERSION_TAG} ${DOCKER_DIR}
