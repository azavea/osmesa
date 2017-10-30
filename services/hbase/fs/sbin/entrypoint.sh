#!/bin/sh

set -euo pipefail
[ -n "${DEBUG:-}" ] && set -x

source /sbin/hbase-lib.sh

: ${ZOOKEEPERS:=zookeeper}
: ${INSTANCE_NAME:=hbase-master}
: ${HADOOP_MASTER_ADDRESS:=hdfs-name}
: ${GEOMESA_VERSION:=1.3.3}

template $HADOOP_CONF_DIR/core-site.xml
template $HADOOP_CONF_DIR/hdfs-site.xml

# The first argument determines this container's role in the hbase cluster
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

      if [[ $ROLE == "master" ]]; then
          if geomesa_jars_installed $HADOOP_MASTER_ADDRESS; then
              echo "GeoMesa JAR installed!"
          else
              echo "Installing GeoMesa JAR"
              put_geomesa_jar $HADOOP_MASTER_ADDRESS;
          fi
      fi

      exec runuser -p -u $USER $HBASE_HOME/bin/hbase -- $ROLE start;;
    *)
      exec "$@"
  esac
  echo "ENDING!"
fi
