#!/bin/bash

set -e

if [[ -n "${OSMESA_DEBUG}" ]]; then
    set -x
fi

function usage() {
    echo -n "Usage: $(basename "${0}") [OPTION]

Login to a running Docker container's shell.

Options:
    app       App container
    database  Database container
    django    Django container
    nginx     Nginx container
    help      Display this help text
"
}

case $1 in
    app|django|nginx) NORMAL_CONTAINER=1 ;;
    database)         DATABASE_CONTAINER=1 ;;
    help|*)           usage; exit 1 ;;
esac

if [ -n "$NORMAL_CONTAINER" ]; then
    docker-compose exec "${1}" /bin/bash
fi

if [ -n "$DATABASE_CONTAINER" ]; then
    docker-compose exec database gosu postgres psql -d osmesa
fi
