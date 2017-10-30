# GeoDocker HDFS

[![Build Status](https://api.travis-ci.org/geodocker/geodocker-hdfs.svg)](http://travis-ci.org/geodocker/geodocker-hdfs)
[![Docker Repository on Quay](https://quay.io/repository/geodocker/base/status "Docker Repository on Quay")](https://quay.io/repository/geodocker/hdfs)
[![Join the chat at https://gitter.im/geotrellis/geotrellis](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/geotrellis/geotrellis?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

HDFS container for [GeoDocker Cluster](https://github.com/geodocker/geodocker)

# Roles
This container has three roles that can be supplied as `CMD`:
  - `name` - HDFS namenode
  - `sname` - HDFS secondary namenode
  - `data` - HDFS data node

# Configuration

Configuration can be done by either providing the required environment variables such that a minimal configuration can be templated or by volume mounting hadoop configuration directory to `/etc/hadoop/conf`

# Environmen
  - `HADOOP_MASTER_ADDRESS` - hostname for HDFS root, required for all roles


# Testing
This container should be tested with `docker-compose` and through `make test`
