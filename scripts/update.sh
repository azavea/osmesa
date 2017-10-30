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
        # Build the base image
        pushd ./services/services-base
        make build
        popd

        # Build services containers.
        docker-compose \
            -f docker-compose.services.yml \
            build

        # Build sbt container.
        docker-compose \
            -f docker-compose.sbt.yml \
            build
    fi
fi
