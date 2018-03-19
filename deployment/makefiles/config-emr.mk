export NAME := OSM Vector Tile Ingest - ${USER}
export USE_SPOT := true
export YARN_OVERHEAD := 1500

export MASTER_INSTANCE:=m3.2xlarge
export MASTER_PRICE := 0.13
export DRIVER_MEMORY := 20000M
export DRIVER_CORES := 8

export WORKER_INSTANCE:=m3.xlarge
export WORKER_PRICE := 0.13
export WORKER_COUNT := 200
export EXECUTOR_MEMORY := 21500M
export EXECUTOR_CORES := 8

# export WORKER_INSTANCE:=m3.xlarge
# export WORKER_PRICE := 0.10
# export WORKER_COUNT := 200
# export EXECUTOR_MEMORY := 10000M
# export EXECUTOR_CORES := 4
