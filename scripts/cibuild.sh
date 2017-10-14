#!/bin/bash

set -e

if [[ -n "${OSMESA_DEBUG}" ]]; then
    set -x
fi

function usage() {
    echo -n "Usage: $(basename "$0")

Build application for staging or a release.
"
}

if [[ -n "${GIT_COMMIT}" ]]; then
    GIT_COMMIT="${GIT_COMMIT:0:7}"
else
    GIT_COMMIT="$(git rev-parse --short HEAD)"
fi

if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    if [ "${1:-}" = "--help" ]; then
        usage
    else
        ./scripts/lint.sh
        ./scripts/test.sh

        # Build static asset bundle
        docker-compose run --rm --no-deps \
                       -e NODE_ENV="${NODE_ENV:-production}" \
                       -e INSTALL_ENV="${INSTALL_ENV:-internal}" \
                       -e VERSION="${GIT_COMMIT}" app \
                       yarn run bundle

        # If the cibuild.d directory exists, source each script it
        # contains. This allows the core cibuild.sh to be extended with
        # project specific scripts.
        if [ -d "./scripts/cibuild.d" ]; then
            for file in ./scripts/cibuild.d/*.sh; do
                source "${file}"
            done
        fi
    fi
fi
