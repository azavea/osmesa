#!/bin/bash

set -e

if [[ -n "${OSMESA_DEBUG}" ]]; then
    set -x
fi

function usage() {
    echo -n "Usage: $(basename "$0")

Downloads the test data for OSMesa
"
}

if [ "${BASH_SOURCE[0]}" = "${0}" ]
then
    if [ "${1:-}" = "--help" ]
    then
        usage
    else
        # Make sure the data directory exists
        mkdir -p ./src/test-data/

        # Download suriname ORC file
        wget -O ./src/test-data/suriname.orc https://s3.amazonaws.com/vectortiles/orc/south-america/suriname.orc
    fi
fi
