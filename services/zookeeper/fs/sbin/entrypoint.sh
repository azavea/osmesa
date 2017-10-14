#! /usr/bin/env bash

set -eo pipefail

ID=${ZOOKEEPER_ID:-0}
echo $ID > ${ZOOKEEPER_DATA}/myid

for VAR in `env`
do
  if [[ $VAR =~ ^ZOOKEEPER_SERVER_[0-9]+= ]]; then
    SERVER_ID=`echo "$VAR" | sed -r "s/ZOOKEEPER_SERVER_(.*)=.*/\1/"`
    SERVER_IP=`echo "$VAR" | sed 's/.*=//'`
    if [ ! $(grep "server.${SERVER_ID}=" ${ZOOKEEPER_CONF_DIR}/zoo.cfg) ]; then
      if [ "${SERVER_ID}" = "${ID}" ]; then
        echo "server.${SERVER_ID}=0.0.0.0:2888:3888" >> ${ZOOKEEPER_CONF_DIR}/zoo.cfg
      else
        echo "server.${SERVER_ID}=${SERVER_IP}:2888:3888" >> ${ZOOKEEPER_CONF_DIR}/zoo.cfg
      fi
    fi
  fi
done

exec "$@"
