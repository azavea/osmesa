#!/bin/bash

set -e

if [[ -n "${OSMESA_DEBUG}" ]]; then
    set -x
fi

function usage() {
    echo -n "Usage: $(basename "$0") [OPTION]

Run linters and tests.

Options:
    -h --help       Display this help text
    --git           Check git commit titles
"
}

function app_tests() {
    # TODO: Run tests
    true;
}

function git_tests() {
    # Fail build if any commit title contains these words
    if git log --oneline | grep -wiE "fixup|squash|wip"; then
        echo "ERROR: Please squash all changes before merging."
        exit 1
    fi
}

if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    case "${1:-}" in
        -h|--help) usage ;;
        --git)     git_tests ;;
        *)         app_tests ;;
    esac
fi
