#!/bin/bash

set -e

if [[ -n "${OSMESA_DEBUG}" ]]; then
    set -x
fi

function usage() {
    echo -n "Usage: $(basename "${0}") [OPTION]

Login to a container's shell.

Options:
    hbase-master    HBase container
    spark           Spark container
"
}

case $1 in
    help)
        usage; exit 1 ;;
    *)
        docker-compose \
            -f docker-compose.sbt.yml \
            -f docker-compose.services.yml \
            exec "${1}" /bin/bash  ;;

esac
