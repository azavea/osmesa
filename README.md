# OSMesa

[![Join the chat at https://gitter.im/osmesa/Lobby](https://badges.gitter.im/osmesa/Lobby.svg)](https://gitter.im/osmesa/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

This project is a collection of tools for working with OpenStreetMap (OSM). It is built to enable
large scale batch analytic jobs to run on the latest OSM data, as well as streaming jobs which
operate on updated with minutely replication files.

## Getting Started

This library is a toolkit meant to make the munging and manipulation of
OSM data a simpler affair than it would otherwise be. Nevertheless, a
significant degree of domain-specific knowledge is necessary to
profitably work with OSM data. Prospective users would do well to study
the OSM data-model and to develop an intuitive sense for how the various
pieces of the project hang together to enable an open-source, globe-scale
map of the world.

If you're already fairly comfortable with OSM's model, running one of
the diagnostic (console printing/debugging) Spark Streaming applications
provided in the analytics subproject is probably the quickest way to
explore Spark SQL and its usage within this library. To run the
[change stream processor](src/analytics/src/main/scala/osmesa/analytics/oneoffs/ChangeStreamProcessor.scala)
application from the beginning of (OSM) time and until cluster failure
or user termination, try this:

```bash
# head into the 'src' directory
cd src

# build the jar we'll be submitting to spark
sbt "project analytics" assembly

# submit the streaming application to spark for process management
spark-submit \
  --class osmesa.analytics.oneoffs.ChangeStreamProcessor \
  ./analytics/target/scala-2.11/osmesa-analytics.jar \
  --start-sequence 1
```

## Deployment

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

- Number of added [buildings](https://wiki.openstreetmap.org/wiki/Buildings) (`building=*`,
  `version=1`)
- Number of modified [buildings](https://wiki.openstreetmap.org/wiki/Buildings) (`building=*`,
  `version > 1 || minorVersion > 0`)
- Number of deleted [buildings](https://wiki.openstreetmap.org/wiki/Buildings) (`building=*`,
  `visible == false`)
- Number of added [roads](https://wiki.openstreetmap.org/wiki/Highways) (`highway=*`,
  `version=1`)
- Number of modified [roads](https://wiki.openstreetmap.org/wiki/Highways) (`highway=*`,
  `version > 1 || minorVersion > 0`)
- Number of deleted [roads](https://wiki.openstreetmap.org/wiki/Highways) (`highway=*`,
  `visible == false`)
- km of added [roads](https://wiki.openstreetmap.org/wiki/Highways) (`highway=*`, `version=1`)
- km of modified [roads](https://wiki.openstreetmap.org/wiki/Highways) (`highway=*`,
  `version > 1 || minorVersion > 0`)
- km of deleted [roads](https://wiki.openstreetmap.org/wiki/Highways) (`highway=*`,
  `visible == false`)
- Number of added [waterways](https://wiki.openstreetmap.org/wiki/Waterways)
  (`waterway={river,riverbank,canal,stream,stream_end,brook,drain,ditch,dam,weir,waterfall,pressurised}`,
  `version=1`)
- Number of modified [waterways](https://wiki.openstreetmap.org/wiki/Waterways)
  (`waterway={river,riverbank,canal,stream,stream_end,brook,drain,ditch,dam,weir,waterfall,pressurised}`,
  `version > 1 || minorVersion > 0`)
- Number of deleted [waterways](https://wiki.openstreetmap.org/wiki/Waterways)
  (`waterway={river,riverbank,canal,stream,stream_end,brook,drain,ditch,dam,weir,waterfall,pressurised}`,
  `version > 1 || minorVersion > 0`)
- km of added [waterways](https://wiki.openstreetmap.org/wiki/Waterways) (`waterway=*`,
  `version=1`)
- km of modified [waterways](https://wiki.openstreetmap.org/wiki/Waterways) (`waterway=*`,
  `version > 1 || minorVersion > 0`)
- km of deleted [waterways](https://wiki.openstreetmap.org/wiki/Waterways) (`waterway=*`,
  `version > 1 || minorVersion > 0`)
- Number of added [coastlines](https://wiki.openstreetmap.org/wiki/Coastline)
  (`natural=coastline`, `version=1`)
- Number of modified [coastlines](https://wiki.openstreetmap.org/wiki/Coastline)
  (`natural=coastline`, `version > 1 || minorVersion > 0`)
- Number of deleted [coastlines](https://wiki.openstreetmap.org/wiki/Coastline)
  (`natural=coastline`, `visible == false`)
- km of added [coastline](https://wiki.openstreetmap.org/wiki/Coastline) (`natural=coastline`,
  `version=1`)
- km of modified [coastline](https://wiki.openstreetmap.org/wiki/Coastline) (`natural=coastline`,
  `version > 1 || minorVersion > 0`)
- km of deleted [coastline](https://wiki.openstreetmap.org/wiki/Coastline) (`natural=coastline`,
  `visible == false`)
- Number of added [points of interest](https://wiki.openstreetmap.org/wiki/Points_of_interest)
  (`{amenity,shop,craft,office,leisure,aeroway}=*`, `version=1`)
- Number of modified [points of interest](https://wiki.openstreetmap.org/wiki/Points_of_interest)
  (`{amenity,shop,craft,office,leisure,aeroway}=*`, `version > 1 || minorVersion > 0`)
- Number of deleted [points of interest](https://wiki.openstreetmap.org/wiki/Points_of_interest)
  (`{amenity,shop,craft,office,leisure,aeroway}*`, `visible == false`)
- Number of added "other" (not otherwise tracked, but tagged in OSM) features (not otherwise
  captured, `version=1`)
- Number of modified "other" features (not otherwise captured, `version > 1 || minorVersion > 0`)
- Number of deleted "other" features (not otherwise captured, `visible == false`)

### SQL Tables

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
[hashtag_statistics](https://github.com/azavea/osmesa-stat-server/blob/master/sql/hashtag_statistics.sql)
and [user_statistics](https://github.com/azavea/osmesa-stat-server/blob/master/sql/user_statistics.sql)

### Batch

- [ChangesetStats](src/analytics/src/main/scala/osmesa/analytics/oneoffs/ChangesetStats.scala)
will produce an ORCfile with statistics aggregated by changeset

- [ChangesetStatsCreator](src/analytics/src/main/scala/osmesa/analytics/oneoffs/ChangesetStatsCreator.scala)
will load aggregated statistics into a PostgreSQL database using JDBC.

### Stream

- [StreamingChangesetStatsUpdater](src/analytics/src/main/scala/osmesa/analytics/oneoffs/StreamingChangesetStatsUpdater.scala)
updates the tables `changesets`, `users`, and `changesets_countries` using geometries available from the augmented diff stream.
- [StreamingChangesetMetaUpdater](src/analytics/src/main/scala/osmesa/analytics/oneoffs/StreamingChangesetMetaUpdater.scala)
updates the tables `changesets`, `changesets_hashtags`, `users`, and `hashtags` using metadata in the changesets stream.
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

### Batch

- [FootprintByCampaign](src/analytics/src/main/scala/osmesa/analytics/oneoffs/FootprintByCampaign.scala)
produces a z/x/y stack of vector tiles corresponding to all changes
marked with a given hashtag
- [FootprintByUser](src/analytics/src/main/scala/osmesa/analytics/oneoffs/FootprintByUser.scala)
produces a z/x/y stack of vector tiles which correspond to a user's
modifications to OSM

### Stream

- [HashtagFootprintUpdater](src/analytics/src/main/scala/osmesa/analytics/oneoffs/HashtagFootprintUpdater.scala)
updates a z/x/y stack of vector tiles corresponding to all changes
marked with a given hashtag
- [UserFootprintUpdater](src/analytics/src/main/scala/osmesa/analytics/oneoffs/UserFootprintUpdater.scala)
updates a z/x/y stack of vector tiles which correspond to a user's
modifications to OSM
