export NAME := OSM Vector Tile Ingest - ${USER}
export MASTER_SECURITY_GROUP := sg-05ce84aaf4b8ddea9
export WORKER_SECURITY_GROUP := sg-02edec736e909d506
export SERVICE_ACCESS_SG := sg-055b18600d3fc2c50
export SANDBOX_SG := sg-6b227c23

export MASTER_INSTANCE := m3.xlarge
export MASTER_PRICE := 0.10
export WORKER_INSTANCE := m3.xlarge
export WORKER_PRICE := 0.07
export WORKER_COUNT := 32
export USE_SPOT := true

export DRIVER_MEMORY := 10000M
export DRIVER_CORES := 4
export EXECUTOR_MEMORY := 10000M
export EXECUTOR_CORES := 8
export YARN_OVERHEAD := 1500
