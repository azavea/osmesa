## OSMesa

[![Join the chat at https://gitter.im/osmesa/Lobby](https://badges.gitter.im/osmesa/Lobby.svg)](https://gitter.im/osmesa/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

This project is a collection of tools for working with OpenStreetMap (OSM). It is built to enable
large scale batch analytic jobs to run on the latest OSM data, as well as streaming jobs which
operate on updated with minutely replication files.
__NOTE__ This repo is pre-alpha and under active development. Contact the authors if you are interested in helping out or using this project.


## Batch Analytics

Spark batch jobs will be able to run on ephemeral clusters for analytics, either scheduled or ad-hoc.

GeoMesa SparkSQL will also be available for analytics.


### VectorTile Generation

The OSMesa tools will be able to transform an RDD of Features that is pulled out of OSM ORC files
into Vector Tiles with an appropriate schema.


### Getting Started

Utilities are provided in the [emr directory](emr) which bring up an EMR
cluster and enable you to push the OSMesa uberjar to said cluster. The
spawned EMR cluster comes with Apache Zeppelin enabled, which allows
jars to be registered/loaded for a console-like experience similar to
Jupyter or IPython notebooks but which will execute spark jobs across the
entire spark cluster.

## Streaming statistics

In addition to batch analytics, Spark Streaming processes are provided
to compute statistics and geometries as OSM is updated.


1. [AugmentedDiffStreamProcessor](src/analytics/src/main/scala/osmesa/analytics/oneoffs/AugmentedDiffStreamProcessor.scala)
2. [ChangeSetStreamProcessor](src/analytics/src/main/scala/osmesa/analytics/oneoffs/ChangeSetStreamProcessor.scala)
3. [ChangeStreamProcessor](src/analytics/src/main/scala/osmesa/analytics/oneoffs/ChangeStreamProcessor.scala)
4. [MergedChangesetStreamProcessor](src/analytics/src/main/scala/osmesa/analytics/oneoffs/MergedChangesetStreamProcessor.scala)

## SQL Tables

Statistics calculation, whether batch or streaming, updates a few tables
that jointly can be used to discover user or hashtag stats. These are
the schemas of the tables being updated.

- [changesets](src/analytics/sql/changesets.sql)
- [changesets_countries](src/analytics/sql/changesets_countries.sql)
- [changesets_hashtags](src/analytics/sql/changesets_hashtags.sql)
- [countries](src/analytics/sql/countries.sql)
- [hashtags](src/analytics/sql/hashtags.sql)
- [users](src/analytics/sql/users.sql)


These tables are fairly normalized and thus not the most efficient for
directly supporting statistics servers. To that end, it might be useful
to create materialized views for any furhter aggregation. A couple such
example views are provided:
[hashtag_statistics](src/analytics/sql/materialized_views/hashtag_statistics.sql)
and [user_statistics](src/analytics/sql/materialized_views/user_statistics.sql)

