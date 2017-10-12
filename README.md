## OSMesa

[![Join the chat at https://gitter.im/osmesa/Lobby](https://badges.gitter.im/osmesa/Lobby.svg)](https://gitter.im/osmesa/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

This project is a stack for working with OSM and other vector data sources in GeoMesa. It is build to allow for large scale batch analytic jobs to run on the latest OSM data, updated with minutely replication files.

__NOTE__ This repo is pre-alpha and under active development. Contact the authors if you are interested in helping out or using this project.

## Components

![Architecture](architecture.svg)

### Ingest

Use [osm2orc](https://github.com/mojodna/osm2orc) to convert `pbf` files to `ORC` files.

Use osmesa-ingest to ingest ORC file into the GeoMesa instance.
The GeoMesa instance will be based on HBase, with it's storage on S3.
VectorPipe will be used to generate the feature data from OSM elements.
The ingest will be run as a Spark job.

The ingest will create the following key/value tables:

node-id -> metadata
node-id -> way-id
node-id -> relation-id
way-id -> metadata
relation-id -> metadata
changeset-id -> metadata
way-id -> geometry
relation-id -> geometry

### Update

There is a polling service that watches for diffs and updates the GeoMesa instance accordingly.
This service will also be able to throw augmented diffs on a queue for consumption by [planet-stream](https://github.com/developmentseed/planet-stream).

### Query API

This service will be an akka-http service running on an instance with an HBase read-only master, which
can be used to query the GeoMesa instance on S3. This will only be able to queries of a size that will
be based on the resources available to this instance or cluster.

### Global Analytics

Spark batch jobs will be able to run on ephemeral clusters for analytics, either scheduled or ad-hoc.

GeoMesa SparkSQL will also be available for analytics.

### VectorTile Generation

VectorPipe will be able to transform an RDD of Features that is pulled out of GeoMesa into Vector Tiles
with an appropriate schema.

## Development

You'll need to publish local artifacts for VectorPipe, GeoTrellis and GeoMesa.

### TODO

Development setup that allows you to:
- Create a local ingest of OSMFeature and Geometry features out of ORC into GeoMesa
- Use GeoServer to view results
- Dockerized setup of all components
- Terraform-based deployment scripts for running setup in AWS
