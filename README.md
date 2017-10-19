## OSMesa

[![Join the chat at https://gitter.im/osmesa/Lobby](https://badges.gitter.im/osmesa/Lobby.svg)](https://gitter.im/osmesa/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

This project is a stack for working with Open Street Map (OSM) and other vector data sources in GeoMesa. It is build to allow for large scale batch analytic jobs to run on the latest OSM data, updated with minutely replication files.
__NOTE__ This repo is pre-alpha and under active development. Contact the authors if you are interested in helping out or using this project.

## Components

![Architecture](architecture.svg)

### OSMesa Store

The center of it all is a GeoMesa-enabled instance of HBase, backed by the Amazon S3 object store, that will store all Open Street Map and other

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

Development of OSMesa happens via Docker containers run inside of a Vagrant machine.
While this is a complex setup, it allows us full control over the development environment,
and how it maps to the deployment environment.

__Note__: If you might have permissions issues with Docker writing to the host machine
(some OS's do), you might want to remove the `config.vm.synced_folder` lines in the
`Vagrantfile` and simply copy up credentials to the VM/let the VM have it's own Ivy cache, etc.

### Requirements

* Vagrant 1.8+
* VirtualBox 4.3
* Ansible 2.1+

### Getting Started

The first steps are to build some unpublished binaries:

#### Build GeoMesa and make hbase-dist binaries available

First thing to is to [build and install GeoMesa](https://github.com/locationtech/geomesa#building-from-source).
You can run `mvn clean install -T8 -am -DskipTests` on a clone of GeoMesa to accomplish this.

__IMPORTANT__: Next, move the `geomesa-hbase-dist` installed in your local maven repository. Move it to both these locations:

- `services/hbase/geomesa-hbase-dist.tar.gz`
- `services/geoserver/geomesa-hbase-dist.tar.gz`

For instance, running this if you built GeoMesa and installed locally:
- `cp ~/.m2/repository/org/locationtech/geomesa/geomesa-hbase-dist_2.11/1.4.0-SNAPSHOT/geomesa-hbase-dist_2.11-1.4.0-SNAPSHOT-bin.tar.gz services/hbase/geomesa-hbase-dist.tar.gz`
- `cp ~/.m2/repository/org/locationtech/geomesa/geomesa-hbase-dist_2.11/1.4.0-SNAPSHOT/geomesa-hbase-dist_2.11-1.4.0-SNAPSHOT-bin.tar.gz services/geoserver/geomesa-hbase-dist.tar.gz`


You'll also need the local maven repository to build the scala code in this project; this means that if you
have Vagrant syncing your `~/.m2` folder, you can build directly on your machine, but if
you have disabled that syncing you'll have to build and install it inside of the Vagrant box.

#### Build the latest GeoTrellis

The same thing as above applies here: if Vagrant is syncing to the host `~/.ivy2`, you can publish
local binaries on the host machine; otherwise you'll have to publish-local from the Vagrant box.

- Clone https://github.com/locationtech/geotrellis
- `cd geotrellis`
- `scripts/publish-local.sh`

This one requires that SBT be locally installed.

_TODO:_ Should we bake all these in a docker container to make it easier?

#### Build the latest VectorPipe

- Clone https://github.com/geotrellis/vectorpipe
- `cd vectorpipe`
- `sbt publish-local`

### Building and running project services

```sh
./scripts/setup.sh
```

Rebuild Docker images and run the services.

```sh
vagrant up
vagrant ssh
./scripts/update.sh
./scripts/services.sh
```

### Ports

| Service            | Port                               |
| ------------------ | ---------------------------------- |
| HBase UI           | [`16010`](http://localhost:16010/) |

### Testing (TODO)

```
./scripts/test.sh
```

### Scripts

| Name           | Description                                                   |
| -------------- | ------------------------------------------------------------- |
| `cibuild.sh`   | Build project for CI (TODO)                                   |
| `clean.sh`     | Free disk space by cleaning up dangling Docker images         |
| `console.sh`   | Run interactive shell inside application container            |
| `geoserver.sh` | Run GeoServer in the dev network                              |
| `sbt.sh`       | Run an sbt console in the Spark container in the dev network  |
| `services.sh`  | Run Docker Compose services                                   |
| `setup.sh`     | Provision Vagrant VM and run `update.sh`                      |
| `test.sh`      | Run unit tests       (TODO)                                   |
| `update.sh`    | Build Docker images                                           |

### Docker setup

![Docker Dev setup](docker-dev.svg)

The development environment contains a number of docker-compose files which are used to
run local development versions of the various services that a deployment environment would have,
as  the update/query containers.
Spark jobs will be run by scripts, and bring up a docker container that connects to the default network.
The docker-compose.services.yml is the only one that brings up a docker network, which is named
`osmesa_default`. All other containers need to connect to this network in order to communicate
with the other components.

Below is a description of the various docker-compose files and scripts to run spark jobs:

| Name                            | Description                                           |
| ------------------------------- | ----------------------------------------------------- |
| `docker-compose.services.yml`   | Development services, including GeoMesa-enabled HBase |
| `docker-compose.geoserver.yml`  | Brings up a GeoServer instance in dev network         |
| `docker-compose.sbt.yml`        | Runs a sbt console in the spark-enabled container     |
| `docker-compose.update.yml`     | Container that runs the OSMesa update service [TODO]  |
| `docker-compose.query.yml`      | Container that runs the OSMesa query service  [TODO]  |

### GeoServer

GeoServer runs outside of the normal docker-compose.services.yml, because it tends to take up a lot
of memory, so if you don't need to run it it won't eat up all your resources.

To run, use `script/geoserver.sh` when the `scripts/services.sh` services are already running.
To view data in GeoServer, go to this address:

[http://localhost:9090/geoserver/web](http://localhost:9090/geoserver/web)

Login with `admin`:`geoserver`

To add an HBase layer:
- click 'Stores' in the left gutter
- then 'Add new store'
- then HBase (GeoMesa).

Click 'save'. You should now be able to add GeoMesa layers from HBase.

### TODO

Development setup that allows you to:
- Create a local ingest of OSMFeature and Geometry features out of ORC into GeoMesa
- Use GeoServer to view results
- Dockerized setup of all components
- Terraform-based deployment scripts for running setup in AWS
