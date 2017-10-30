#!/bin/bash

set -e

if [[ -n "${OSMESA_DEBUG}" ]]; then
    set -x
fi

function usage() {
    echo -n "Usage: $(basename "${0}") [OPTION]

Run the sbt container
"
}

docker-compose -f docker-compose.sbt.yml run sbt
