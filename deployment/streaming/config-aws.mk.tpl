export CONFIG_NAME := osm-stat-stream-config

# AWS properties
export CLUSTER_NAME := osm-stat-stream-cluster
export INSTANCE_TYPE := m4.xlarge
export KEYPAIR := [AWS key pair]
export VPC := [VPC ID]
export SUBNETS := [Subnets within the above VPC]
export SECURITY_GROUP := [AWS Security Group ID
export ECR_REPO := [AWS ECR repo URI]
export AWS_LOG_GROUP := osm-stats-stream
export AWS_REGION := us-east-1

export AUGDIFF_SOURCE := s3://path/to/augdiffs/
export CHANGESET_SOURCE := https://planet.osm.org/replication/changeset/
export AUGDIFF_START := [Start of Augdiff stream]
export CHANGESET_START := [Start of changeset stream]

export DB_URI := [URI to DB for writing outputs from stream]

