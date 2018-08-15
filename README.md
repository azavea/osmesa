## OSMesa

[![Join the chat at https://gitter.im/osmesa/Lobby](https://badges.gitter.im/osmesa/Lobby.svg)](https://gitter.im/osmesa/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

This project is a collection of tools for working with OpenStreetMap (OSM). It is built to enable
large scale batch analytic jobs to run on the latest OSM data, as well as streaming jobs which
operate on updated with minutely replication files.


### Getting Started

Utilities are provided in the [deployment directory](deployment) to bring
up cluster and enable you to push the OSMesa jar to that cluster. The
spawned EMR cluster comes with Apache Zeppelin enabled, which allows
jars to be registered/loaded for a console-like experience similar to
Jupyter or IPython notebooks but which will execute spark jobs across the
entire spark cluster. Actually wiring up Zeppelin to use OSMesa sources
is beyond the scope of this document, but it is a relatively simple
configuration.


## Statistics

Summary statistics aggregated at the user and hashtag level that are
supported by OSMesa:

- Number of added buildings
- Number of modified buildings
- Number of added roads
- Number of modified roads
- Km of added roads
- Km of modified roads
- Number of added waterways
- Number of modified waterways
- Km of added waterways
- Km of modified waterways
- Number of added points of interest
- Number of modified points of interest

#### SQL Tables

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
directly serving statistics. If that's your goal, it might be useful
to create materialized views for any further aggregation. A couple example
queries that can serve as views are provided:
[hashtag_statistics](src/analytics/sql/materialized_views/hashtag_statistics.sql)
and [user_statistics](src/analytics/sql/materialized_views/user_statistics.sql)

#### Batch

- [ChangesetStats](src/analytics/src/main/scala/osmesa/analytics/oneoffs/ChangesetStats.scala)
will produce an ORCfile with statistics aggregated by changeset

#### Stream

- [AugmentedDiffStreamProcessor](src/analytics/src/main/scala/osmesa/analytics/oneoffs/AugmentedDiffStreamProcessor.scala)
updates the tables `changesets`, `users`, and `changesets_countries`
- [ChangeSetStreamProcessor](src/analytics/src/main/scala/osmesa/analytics/oneoffs/ChangeSetStreamProcessor.scala)
updates the tables `changesets`, `changesets_hashtags`, `users`, and `hashtags`
- [ChangeStreamProcessor](src/analytics/src/main/scala/osmesa/analytics/oneoffs/ChangeStreamProcessor.scala)
prints out changes to the console (primarily for debugging)
- [MergedChangesetStreamProcessor](src/analytics/src/main/scala/osmesa/analytics/oneoffs/MergedChangesetStreamProcessor.scala)
prints out changesets to the console (primarily for debugging)


## Vector Tiles

Vector tiles, too, are generated in batch and via streaming so that a
fresh set can be quickly generated and then kept up to date. Summary
vector tiles are produced for two cases: to illustrate the scope
of a user's contribution and to illustrate the scope of a
hashtag/campaign within OSM

#### Batch

- [FootprintByCampaign](src/analytics/src/main/scala/osmesa/analytics/oneoffs/FootprintByCampaign.scala)
produces a z/x/y stack of vector tiles corresponding to all changes
marked with a given hashtag
- [FootprintByUser](src/analytics/src/main/scala/osmesa/analytics/oneoffs/FootprintByUser.scala)
produces a z/x/y stack of vector tiles which correspond to a user's
modifications to OSM

#### Stream

- [HashtagFootprintUpdater](src/analytics/src/main/scala/osmesa/analytics/oneoffs/HashtagFootprintUpdater.scala)
updates a z/x/y stack of vector tiles corresponding to all changes
marked with a given hashtag
- [UserFootprintUpdater](src/analytics/src/main/scala/osmesa/analytics/oneoffs/UserFootprintUpdater.scala)
updates a z/x/y stack of vector tiles which correspond to a user's
modifications to OSM


