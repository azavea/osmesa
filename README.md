# OSMesa

[![Join the chat at https://gitter.im/osmesa/Lobby](https://badges.gitter.im/osmesa/Lobby.svg)](https://gitter.im/osmesa/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

This project is a collection of tools for working with OpenStreetMap (OSM). It is built to enable
large scale batch analytic jobs to run on the latest OSM data, as well as streaming jobs which
operate on updated with minutely replication files.

All command apps written to perform these batch and streaming jobs live in the `apps` subproject.
Reusable components that can be used to construct your own batch and stream processors live
in the `analytics` subproject.

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
provided in the apps subproject is probably the quickest way to
explore Spark SQL and its usage within this library. To run the
[change stream processor](src/apps/src/main/scala/osmesa/apps/streaming/ChangeStreamProcessor.scala)
application from the beginning of (OSM) time and until cluster failure
or user termination, try this:

```bash
# head into the 'src' directory
cd src

# build the jar we'll be submitting to spark
sbt "project apps" assembly

# submit the streaming application to spark for process management
spark-submit \
  --class osmesa.apps.streaming.ChangeStreamProcessor \
  ./apps/target/scala-2.11/osmesa-apps.jar \
  --start-sequence 1
```

## Deployment

Utilities are provided in the [deployment directory](deployment) to bring
up cluster and enable you to push the OSMesa apps jar to that cluster. The
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

- [changesets](deployment/sql/changesets.sql)
- [changesets_countries](deployment/sql/changesets_countries.sql)
- [changesets_hashtags](deployment/sql/changesets_hashtags.sql)
- [countries](deployment/sql/countries.sql)
- [hashtags](deployment/sql/hashtags.sql)
- [users](deployment/sql/users.sql)


These tables are fairly normalized and thus not the most efficient for
directly serving statistics. If that's your goal, it might be useful
to create materialized views for any further aggregation. A couple example
queries that can serve as views are provided:
[hashtag_statistics](https://github.com/azavea/osmesa-stat-server/blob/master/sql/hashtag_statistics.sql)
and [user_statistics](https://github.com/azavea/osmesa-stat-server/blob/master/sql/user_statistics.sql)

### Batch

- [ChangesetStatsCreator](src/apps/src/main/scala/osmesa/apps/batch/ChangesetStatsCreator.scala)
will load aggregated statistics into a PostgreSQL database using JDBC.

- [MergeChangesets](src/apps/src/main/scala/osmesa/apps/batch/MergeChangesets.scala)
will update a changesets ORC file with the contents of the provided changeset stream.

### Stream

- [StreamingChangesetStatsUpdater](src/apps/src/main/scala/osmesa/apps/streaming/StreamingChangesetStatsUpdater.scala)
updates the tables `changesets`, `users`, and `changesets_countries` using geometries available from the augmented diff stream.
- [StreamingChangesetMetadataUpdater](src/apps/src/main/scala/osmesa/apps/streaming/StreamingChangesetMetadataUpdater.scala)
updates the tables `changesets`, `changesets_hashtags`, `users`, and `hashtags` using metadata in the changesets stream.
- [ChangeStreamProcessor](src/apps/src/main/scala/osmesa/apps/streaming/ChangeStreamProcessor.scala)
prints out changes to the console (primarily for debugging)
- [MergedChangesetStreamProcessor](src/apps/src/main/scala/osmesa/apps/streaming/MergedChangesetStreamProcessor.scala)
prints out changesets to the console (primarily for debugging)

## Vector Tiles

Vector tiles, too, are generated in batch and via streaming so that a
fresh set can be quickly generated and then kept up to date. Summary
vector tiles are produced for two cases: to illustrate the scope
of a user's contribution and to illustrate the scope of a
hashtag/campaign within OSM

### Batch

- [FootprintCreator](src/apps/src/main/scala/osmesa/apps/batch/FootprintCreator.scala)
produces a z/x/y stack of vector tiles corresponding to all changes
marked with a given hashtag or user, depending on the CLI options provided.

### Stream

- [HashtagFootprintUpdater](src/apps/src/main/scala/osmesa/apps/streaming/HashtagFootprintUpdater.scala)
updates a z/x/y stack of vector tiles corresponding to all changes
marked with a given hashtag
- [UserFootprintUpdater](src/apps/src/main/scala/osmesa/apps/streaming/UserFootprintUpdater.scala)
updates a z/x/y stack of vector tiles which correspond to a user's
modifications to OSM
