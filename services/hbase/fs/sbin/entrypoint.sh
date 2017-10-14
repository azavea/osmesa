#!/bin/sh

set -euo pipefail
[ -n "${DEBUG:-}" ] && set -x

source /sbin/hbase-lib.sh

: ${ZOOKEEPERS:=zookeeper}
: ${INSTANCE_NAME:=hbase}
: ${HADOOP_MASTER_ADDRESS:=hdfs-name}

template $HADOOP_CONF_DIR/core-site.xml
template $HADOOP_CONF_DIR/hdfs-site.xml

# # shell breaks and doesn't run zookeeper without this
# rm -rf $HBASE_HOME/logs
# mkdir -p $HBASE_HOME/logs

# hbase zookeeper > logzoo.log 2>&1 &
# hbase regionserver start > logregion.log 2>&1 &

# hbase master start --localRegionServers=0

# tries to run zookeepers.sh distributed via SSH, run zookeeper manually instead now
# RUN sed -i 's/# export HBASE_MANAGES_ZK=true/export HBASE_MANAGES_ZK=true/' /hbase/conf/hbase-env.sh
#$HBASE_HOME/bin/hbase zookeeper &>$HBASE_HOME/logs/zookeeper.log &
#$HBASE_HOME/bin/start-hbase.sh
#$HBASE_HOME/bin/hbase-daemon.sh start rest
#$HBASE_HOME/bin/hbase-daemon.sh start thrift
# /hbase/bin/hbase-daemon.sh start thrift2
# /hbase/bin/hbase shell
# /hbase/bin/stop-hbase.sh
# pkill -f -i zookeeper

#tail -f ${HBASE_HOME}/logs/*.{log,out}

# # The first argument determines this container's role in the accumulo cluster
ROLE=${1:-}
if [ -z $ROLE ]; then
  echo "Select the role for this container with the docker cmd 'master', 'regionserver', 'rest'"
  exit 1
else
  case $ROLE in
    "master" | "regionserver" | "rest")
      ATTEMPTS=7 # ~2 min before timeout failure
      with_backoff zookeeper_is_available "$ZOOKEEPERS" || exit 1
      wait_until_hdfs_is_available || exit 1

      USER=${USER:-root}
      ensure_user $USER
      echo "Running as $USER"

      # Initialize Accumulo if required
      # if [[ ($ROLE = "master") && (${2:-} = "--auto-init")]]; then
      #   if hbase_instance_exists $INSTANCE_NAME $ZOOKEEPERS; then
      #     echo "Found hbase instance at: $INSTANCE_VOLUME"
      #   else
      #     echo "Initializing hbase instance $INSTANCE_VOLUME ..."
      #     runuser -p -u $USER hdfs -- dfs -mkdir -p ${INSTANCE_VOLUME}-classpath
      #     runuser -p -u $USER accumulo -- init --instance-name ${INSTANCE_NAME} --password ${ACCUMULO_PASSWORD}

      #     if [[ -n ${POSTINIT:-} ]]; then
      #        echo "Post-initializing accumulo instance $INSTANCE_VOLUME ..."
      #        (setsid $POSTINIT &> /tmp/${INSTANCE_NAME}-postinit.log &)
      #     fi
      #   fi
      # fi

      # if [[ $ROLE != "master" ]]; then
      #   with_backoff hbase_instance_exists $INSTANCE_NAME $ZOOKEEPERS || exit 1
      # fi

      exec runuser -p -u $USER $HBASE_HOME/bin/hbase -- $ROLE start;;
    *)
      exec "$@"
  esac
fi
