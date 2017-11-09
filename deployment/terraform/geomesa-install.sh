#!/usr/bin/env bash
#
# Bootstrap docker and accumulo on EMR cluster
#

set -e

VERSION="1.3.4"
JAR_URL="https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-${VERSION}/geomesa-hbase-dist_2.11-${VERSION}-bin.tar.gz"

wget $JAR_URL -O /tmp/geomesa-hbase_2.11-${VERSION}-bin.tar.gz

sudo tar -zxvf /tmp/geomesa-hbase_2.11-${VERSION}-bin.tar.gz -C /opt/

sudo ln -s /opt/geomesa-hbase_2.11-${VERSION} /opt/geomesa

sudo /opt/geomesa/bin/bootstrap-geomesa-hbase-aws.sh
