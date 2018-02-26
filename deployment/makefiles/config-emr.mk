export NAME := OSM Vector Tile Ingest - ${USER}
export MASTER_INSTANCE:=m3.xlarge
export MASTER_PRICE := 0.10
export WORKER_INSTANCE:=m3.xlarge
export WORKER_PRICE := 0.10
export WORKER_COUNT := 128
export USE_SPOT := true

export DRIVER_MEMORY := 10000M
export DRIVER_CORES := 4
export EXECUTOR_MEMORY := 10000M
export EXECUTOR_CORES := 8
export YARN_OVERHEAD := 1500
