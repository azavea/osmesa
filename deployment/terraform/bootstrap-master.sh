#!/bin/bash
set -e

# Parses a configuration file put in place by EMR to determine the role of this node
is_master() {
  if [ $(jq '.isMaster' /mnt/var/lib/info/instance.json) = 'true' ]; then
    return 0
  else
    return 1
  fi
}

if is_master; then
  # install docker on EMR
  sudo yum update -y
  sudo yum install -y docker
  sudo service docker start
  sudo usermod -a -G docker hadoop
fi
