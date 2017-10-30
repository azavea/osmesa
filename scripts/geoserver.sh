#!/bin/bash

set -e

if [[ -n "${OSMESA_DEBUG}" ]]; then
    set -x
fi

function usage() {
    echo -n "Usage: $(basename "${0}")

Run the geoserver container
"
}

docker-compose -f docker-compose.geoserver.yml up
