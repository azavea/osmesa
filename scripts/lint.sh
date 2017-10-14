#!/bin/bash

set -e

if [[ -n "${OSMESA_DEBUG}" ]]; then
    set -x
fi

function usage() {
    echo -n "Usage: $(basename "$0")

Run linters on the project's code
"
}

if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    if [ "${1:-}" = "--help" ]; then
        usage
    else
        # Lint Bash scripts
        if which shellcheck > /dev/null; then
            shellcheck scripts/*.sh
        fi

        # Lint JavaScript
        docker-compose \
            -f docker-compose.yml \
            run --rm --entrypoint ./node_modules/.bin/eslint \
            app js/src/ --ext .js --ext .jsx
    fi
fi
