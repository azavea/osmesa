#!/bin/sh

set -euo pipefail
[ -n "${DEBUG:-}" ] && set -x

source /sbin/hbase-lib.sh

: ${ZOOKEEPERS:=zookeeper}
: ${INSTANCE_NAME:=hbase-master}
: ${HADOOP_MASTER_ADDRESS:=hdfs-name}

template $HADOOP_CONF_DIR/core-site.xml
template $HADOOP_CONF_DIR/hdfs-site.xml

cd /opt/src
./sbt $@
