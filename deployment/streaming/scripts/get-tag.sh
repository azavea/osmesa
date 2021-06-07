#!/bin/bash

if [ "$(git branch | grep '* master')" = "* master" ]; then
    while true; do
	>&2 echo "You are on the master branch.  Do you wish to publish to the production tag?"
	select yn in "Yes" "No"; do
	    case $yn in
		Yes ) VERSION_TAG="production"; break;;
		No ) VERSION_TAG="latest"; break;;
	    esac
	done
        break
    done
else
    if [ -z ${OVERRIDE_TAG+x} ]; then
        VERSION_TAG="latest"
    else
        VERSION_TAG=${OVERRIDE_TAG}
    fi
fi

echo -n "${VERSION_TAG}"
