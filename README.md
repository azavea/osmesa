## OSMesa

This project is a stack for working with OSM and other vector data sources in GeoMesa.

## Components

### Ingest

Use `osm2orc` to convert `pbf` files to `ORC` files.

Use osmesa-import to import ORC file into GeoMesa instance.
The GeoMesa instance will be based on HBase, with it's storage on S3.
VectorPipe will be used to generate the feature data from OSM elements.
The ingest will be run as a Spark job.

The ingest will create the following key/value tables:

node-id -> metadata
way-id -> metadata
relation-id -> metadata
changeset-id -> metadata
way-id -> geometry
relation-id -> geometry

### Update

There is a polling service that watches for diffs and updates the GeoMesa instance accordingly.
This service will also be able to throw augmented diffs on a queue for consumption by `planet-stream`.

### Query Service

This service will be an akka-http service running on an instance with an HBase read-only master, which
can be used to query the GeoMesa instance on S3. This will only be able to queries of a size that will
be based on the resources available to this instance or cluster.

### Analytics

Spark batch jobs will be able to run on ephemeral clusters for analytics, either scheduled or ad-hoc.

GeoMesa SparkSQL will also be available for analytics.

### Generating Vector Tiles

VectorPipe will be able to transform an RDD of Features that is pulled out of GeoMesa into Vector Tiles
with an appropriate schema.

## Development

Use https://github.com/lossyrob/cloud-local/tree/feature/vm-hbase to build a vagrant box.

You'll need to publish local artifacts for VectorPipe, GeoTrellis and GeoMesa.



### Local development ingest setup

- Create a local ingest of OSMFeature and Geometry features into GeoWave.
- Use GeoServer to view results
