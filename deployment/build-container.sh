#!/bin/bash

set -xe
SBT_DIR="../src"
JAR_DIR=${SBT_DIR}/analytics/target/scala-2.11/
DOCKER_DIR=$(pwd)

cd ${SBT_DIR}
./sbt clean "project analytics" assembly
cp ${JAR_DIR}/osmesa-analytics.jar ${DOCKER_DIR}/osmesa-analytics.jar

cd ${DOCKER_DIR}
docker build -f ${DOCKER_DIR}/Dockerfile --tag osm_analytics:latest ${DOCKER_DIR}
