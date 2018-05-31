#!/usr/bin/env bash

set -eo pipefail

OPTIND=1         # Reset in case getopts has been used previously in the shell.

replication_source=""
tile_source=""

while getopts "r:s:t:" opt; do
  case "$opt" in
  r) replication_source=$OPTARG
    ;;
  s) sequence=$OPTARG
    ;;
  t) tile_source=$OPTARG
    ;;
  esac
done

shift $((OPTIND-1))

[ "$1" = "--" ] && shift

if [[ -z $sequence ]]; then
  sequence=$(aws s3 cp ${tile_source}sequence.txt - 2> /dev/null)
else
  sequence=$[$sequence - 1]
fi

if [[ "$sequence" == "-1" || -z $replication_source || -z $tile_source ]]; then
  echo "Usage: $0 -r <replication source> -t <tile source> -s [initial sequence] -- [update-tiles options]"
  exit 1
fi

echo "Starting at sequence $(echo $[$sequence + 1])"

while true; do
  set +e
  aws s3 ls ${replication_source}$((sequence + 1)).json > /dev/null
  retcode=$?
  set -e

  if [[ $retcode -eq 0 ]]; then
    sequence=$[$sequence + 1]

    $(dirname $0)/update-tiles -r $replication_source -t $tile_source -s urchn -l history -v $* $sequence

    echo $sequence | aws s3 cp - ${tile_source}sequence.txt
  else
    echo Waiting for $((sequence + 1))...
    sleep 15
  fi
done
