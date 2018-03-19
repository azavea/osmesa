# export S3_BUCKET:=osmesa-vectortiles
export S3_BUCKET:=osmesa
export S3_URI:=s3://${S3_BUCKET}
export S3_CATALOG := ${S3_URI}/input/gt-processed-dem

export PLANET_ORC := s3://osm-pds/planet-history/history-latest.orc
export NORTH_AMERICA_ORC := ${S3_URI}/orc/north-america-latest.osm.orc
export SPAIN_ORC := ${S3_URI}/orc/spain.osm.orc
export KENYA_ORC := ${S3_URI}/orc/kenya-latest.osm.orc
export ORC_CACHE_LOCATION := ${S3_URI}/new-cache
export VECTORTILE_CATALOG_LOCATION = ${S3_URI}/vectortiles
