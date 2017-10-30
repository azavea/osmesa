#!/bin/bash

set -e

if [[ -n "${OSMESA_DEBUG}" ]]; then
    set -x
fi

function usage() {
    echo -n "Usage: $(basename "$0")

Attempts to setup the project's development environment.
"
}

if [ "${BASH_SOURCE[0]}" = "${0}" ]
then
    if [ "${1:-}" = "--help" ]
    then
        usage
    else
        vagrant up --provision
        vagrant ssh -c "cd /vagrant && ./scripts/download-test-data.sh"
        vagrant ssh -c "cd /vagrant && ./scripts/update.sh"
    fi
fi
