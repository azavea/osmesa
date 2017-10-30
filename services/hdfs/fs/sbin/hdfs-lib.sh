#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# Render a file as template, performing variable substetutions but leaving quotes unchanged
# Credit: http://stackoverflow.com/questions/2914220/bash-templating-how-to-build-configuration-files-from-templates-with-bash
function render_template() {
eval "cat <<EOF
$(<$1)
EOF
"
}

function template() {
  local FILE=$1
  if [ -f $FILE ]; then
    echo "Found config: $FILE"
  else
    local TEMPLATE=${2:-$FILE.template}
    echo "Template config: $FILE from $TEMPLATE"
    render_template $TEMPLATE > $FILE
  fi
}

# Avoid race conditions and actually poll for availability of component dependencies
# Credit: http://stackoverflow.com/questions/8350942/how-to-re-run-the-curl-command-automatically-when-the-error-occurs/8351489#8351489
function with_backoff() {
  local max_attempts=${ATTEMPTS-5}
  local timeout=${INTIAL_POLLING_INTERVAL-1}
  local attempt=0
  local exitCode=0

  while [ $attempt -le $max_attempts ]; do
    echo "Attempt $attempt of $max_attempts: $@"
    set +e
    "$@"
    exitCode=$?
    set -e

    if [[ $exitCode == 0 ]]
    then
      break
    fi

    echo "Retrying $@ in $timeout.." 1>&2
    sleep $timeout
    attempt=$(( attempt + 1 ))
    timeout=$(( timeout * 2 ))
  done

  if [[ $exitCode != 0 ]]; then
    echo "Fail: $@ failed to complete after $max_attempts attempts" 1>&2
  elif [[ $exitCode -gt 128 ]]; then
    echo "Fail: $@ aborted by user" 1>&2
  else
    echo "Success: $@ completed after $attempt attempts" 1>&2
  fi

  return $exitCode
}

function is_port_open() {
  if [[ $(nmap -sT $1 -p $2 --host-timeout 1m) == *"open"* ]]; then
    return 0
  else
    return 1
  fi
}

function wait_until_port_open() {
  echo "Checking for TCP connection to $1:$2..." 1>&2
  with_backoff is_port_open $1 $2
}

hdfs_is_available() {
  echo "Checking for HDFS availability"
	hdfs dfs -test -d / > /dev/null
	return $?
}

function wait_until_hdfs_is_available() {
  with_backoff hdfs dfsadmin -safemode wait
	with_backoff hdfs_is_available
	if [ hdfs_is_available == 0 ]; then
		echo "HDFS not available before timeout. Exiting ..." 1>&2
		exit 1
  else
    echo "HDFS is now available ..." 1>&2
	fi
}
