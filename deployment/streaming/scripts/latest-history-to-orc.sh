#!/bin/bash

# Install/build osm2orc
cd /mnt
sudo yum install -y git
git clone https://github.com/mojodna/osm2orc.git
cd osm2orc
./gradlew distTar
tar xf build/distributions/osm2orc-*.tar -C /tmp

# Download latest planet history file
aws s3 cp $PLANET_HISTORY_PBF /mnt

# Convert to ORC
DATE=$(stat /mnt/planet-history.osm.pbf | sed -n -e 's/-//g;s/Modify: \([0-9\-]*\).*/\1/p')
/tmp/osm2orc-*/bin/osm2orc /mnt/planet-history.osm.pbf /mnt/planet-${DATE}.osh.orc

# Upload ORC
aws s3 cp /mnt/planet-${DATE}.osh.orc $PLANET_HISTORY_ORC_DIR
