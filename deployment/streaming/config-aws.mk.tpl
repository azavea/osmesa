export CONFIG_NAME := osm-stat-stream-config

# AWS properties
export CLUSTER_NAME := osm-stat-stream-cluster
export INSTANCE_TYPE := m4.xlarge
export KEYPAIR := [AWS key pair]
export VPC := [VPC ID]
export SUBNETS := [comma-delimited list of subnets within the above VPC]
export SECURITY_GROUP := [comma-delimited list of AWS Security Group IDs]
export ECR_REPO := [AWS ECR repo URI]
export AWS_LOG_GROUP := osm-stats-stream
export AWS_REGION := us-east-1
export INSTANCE_ROLE := [IAM instance role]

export AUGDIFF_SOURCE := s3://path/to/augdiffs/
export CHANGESET_SOURCE := https://planet.osm.org/replication/changesets/
export AUGDIFF_START := [Start of Augdiff stream]
export CHANGESET_START := [Start of changeset stream]

export DB_URI := [URI to DB for writing outputs from stream]

