#!/bin/bash

set -x

replicas=$(docker container ls -aq)
if [ -z "$replicas" ]; then
    exit 0
fi

# Stop and remove all replicas
# Remove subnet and docker image
docker container stop $replicas
docker container rm $replicas
docker network rm mynet
docker image prune -af
