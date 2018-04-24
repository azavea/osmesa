## OSMesa

[![Join the chat at https://gitter.im/osmesa/Lobby](https://badges.gitter.im/osmesa/Lobby.svg)](https://gitter.im/osmesa/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

This project is a collection of tools for working with OpenStreetMap (OSM). It is built to enable
large scale batch analytic jobs to run on the latest OSM data, as well as streaming jobs which
operate on updated with minutely replication files.
__NOTE__ This repo is pre-alpha and under active development. Contact the authors if you are interested in helping out or using this project.


### Global Analytics

Spark batch jobs will be able to run on ephemeral clusters for analytics, either scheduled or ad-hoc.

GeoMesa SparkSQL will also be available for analytics.


### VectorTile Generation

The OSMesa tools will be able to transform an RDD of Features that is pulled out of OSM ORC files
into Vector Tiles with an appropriate schema.


## Getting Started

Utilities are provided in the [emr directory](emr) which bring up an EMR
cluster and enable you to push the OSMesa uberjar to said cluster. The
spawned EMR cluster comes with Apache Zeppelin enabled, which allows
jars to be registered/loaded for a console-like experience similar to
Jupyter or IPython notebooks but which will execute spark jobs across the
entire spark cluster.


