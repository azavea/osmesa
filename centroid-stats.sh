#!/bin/bash

ROOT_DIR=$(PWD)
echo $ROOT_DIR
cd src
sbt "project analytics" assembly
zip -d analytics/target/scala-2.11/osmesa-analytics.jar 'META-INF/*.SF' 'META-INF/*.RSA' 'META-INF/*SF'
docker run \
  -v /tmp:/tmp/data \
  -v $ROOT_DIR/src/analytics/target/scala-2.11/osmesa-analytics.jar:/opt/osmesa-analytics.jar \
  -p 4040:4040 \
  bde2020/spark-master:2.3.1-hadoop2.7 \
    /spark/bin/spark-submit --class osmesa.analytics.oneoffs.CentroidStats /opt/osmesa-analytics.jar \
    --history file:///tmp/data/isle-of-man-internal.osh.orc \
    --output file:///tmp/data/stats-test.orc
