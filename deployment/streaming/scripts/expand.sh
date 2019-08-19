#!/bin/sh

set -e

PROG=$(basename $0)

usage()
{
    echo "${PROG} <template-file>"
}

expand()
{
    local template="$(cat $1)"
    eval "echo \"${template}\""
}

case $# in
    1) expand "$1";;
    *) usage; exit 0;;
esac
