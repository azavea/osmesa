#!/bin/bash

set -e

docker-compose -f docker-compose.cli.yml run cli \
  spark-submit --class osmesa.ingest.WayDiff ingest/target/scala-2.11/osmesa-ingest.jar "$@"

