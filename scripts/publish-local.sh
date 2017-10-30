#!/bin/bash

set -e

if [[ -n "${OSMESA_DEBUG}" ]]; then
    set -x
fi

SBT_SCRIPT=https://raw.githubusercontent.com/locationtech/geotrellis/96975b45ed1d32cb3075c30ecb4023bfea540ec1/sbt

function usage() {
    echo -n "Usage: $(basename "$0") [OPTIONS]

Clones, builds and locally publishes a project

OPTIONS:
    geotrellis - GeoTrellis master
    geomesa - GeoMesa master
    vectorpipe - VectorPipe master
    all - Build and publish all of the above
"
}

if [ "${BASH_SOURCE[0]}" = "${0}" ]
then
    if [ "${1:-}" = "--help" ]
    then
        usage
    else
        case $1 in
            geotrellis) PUBLISH_GEOTRELLIS=1 ;;
            geomesa)    PUBLISH_GEOMESA=1 ;;
            vectorpipe) PUBLISH_VECTORPIPE=1 ;;
            all)
                PUBLISH_GEOTRELLIS=1
                PUBLISH_GEOMESA=1
                PUBLISH_VECTORPIPE=1
                ;;
            help|*)           usage; exit 1 ;;
        esac

        mkdir -p $HOME/third_party_sources
        pushd $HOME/third_party_sources

        if [ -n "$PUBLISH_GEOTRELLIS" ]; then
            if [ ! -d "geotrellis" ]; then
                git clone https://github.com/locationtech/geotrellis.git geotrellis
            fi
            pushd geotrellis

            git pull
            ./scripts/publish-local.sh

            popd
        fi

        if [ -n "$PUBLISH_GEOMESA" ]; then
            if [ ! -d "geomesa" ]; then
                git clone https://github.com/locationtech/geomesa.git geomesa
            fi
            pushd geomesa

            git pull
            mvn clean install -T8 -am -Dmaven.test.skip;

            popd

            # Bring the hbase-dist tarball to the proper locations
            cp ~/.m2/repository/org/locationtech/geomesa/geomesa-hbase-dist_2.11/1.4.0-SNAPSHOT/geomesa-hbase-dist_2.11-1.4.0-SNAPSHOT-bin.tar.gz ../services/hbase/geomesa-hbase-dist.tar.gz
            cp ~/.m2/repository/org/locationtech/geomesa/geomesa-hbase-dist_2.11/1.4.0-SNAPSHOT/geomesa-hbase-dist_2.11-1.4.0-SNAPSHOT-bin.tar.gz ../services/geoserver/geomesa-hbase-dist.tar.gz
        fi

        if [ -n "$PUBLISH_VECTORPIPE" ]; then
            if [ ! -d "vectorpipe" ]; then
                git clone https://github.com/geotrellis/vectorpipe.git vectorpipe
            fi
            pushd vectorpipe

            git pull
            wget -O sbt $SBT_SCRIPT
            chmod a+x sbt
            ./sbt publishLocal

            popd
        fi

        popd
    fi
fi
