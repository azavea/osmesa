#!/bin/bash

set -e

if [[ -n "${OSMESA_DEBUG}" ]]; then
    set -x
fi

function usage() {
    echo -n "Usage: $(basename "$0")

Builds and pulls container images using docker-compose.
"
}

if [ "${BASH_SOURCE[0]}" = "${0}" ]
then
    if [ "${1:-}" = "--help" ]
    then
        usage
    else
        # Build services containers.
        docker-compose \
            -f docker-compose.services.yml \
            build

        pushd ./src
          sbt "project query" assembly
        popd
        [ -e ./services/query/osmesa-query.jar ] && rm ./services/query/osmesa-query.jar
        ln ./src/query/target/scala-2.11/osmesa-query.jar ./services/query/osmesa-query.jar

        # Build client container.
        docker-compose \
            -f docker-compose.cli.yml \
            build
    fi
fi
