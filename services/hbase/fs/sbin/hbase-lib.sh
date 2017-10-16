#!/usr/bin/env bash

source /sbin/hdfs-lib.sh

HBASE_JAR_DIR=HBase/HFiles/lib
GEOMESA_JAR_LOCATION=$HBASE_JAR_DIR/geomesa-hbase-distributed-runtime.jar

function hbase_instance_exists() {
    local INSTANCE=$1
    local ZOOKEEPERS=$2
    local LS=$(zookeeper-client -server $ZOOKEEPERS ls /hbase/master/$INSTANCE 2>&1 > /dev/null)
    if [[ $LS == *"does not exist"* ]]; then
        return 1
    else
        return 0
    fi
}

function wait_until_hbase_is_available() {
    local INSTANCE=$1
    local ZOOKEEPERS=$2
    wait_until_hdfs_is_available
    with_backoff hbase_instance_exists $INSTANCE $ZOOKEEPERS
}

function zookeeper_is_available(){
    local ZOOKEEPERS=$1
    [ $(nc ${ZOOKEEPERS} 2181 <<< ruok) == imok ]
    return $?
}

function ensure_user() {
    if [ ! $(id -u $1) ]; then useradd $1 -g root; fi
}

function geomesa_jars_installed() {
    local HADOOP_MASTER_ADDRESS=$1
    local LS=$(hdfs dfs -ls hdfs://${HADOOP_MASTER_ADDRESS}/$GEOMESA_JAR_LOCATION 2>&1 > /dev/null)
    if [[ $LS == *"No such file or directory"* ]]; then
        return 1
    else
        return 0
    fi
}

function put_geomesa_jar() {
    local HADOOP_MASTER_ADDRESS=$1
    local JAR=/opt/hbase/lib/geomesa-dist.jar
    hdfs dfs -mkdir -p hdfs://${HADOOP_MASTER_ADDRESS}/$HBASE_JAR_DIR
    hdfs dfs -put $JAR hdfs://${HADOOP_MASTER_ADDRESS}/$GEOMESA_JAR_LOCATION
}
