#!/bin/bash

# Need to modify
set -x

docker network rm mynet
docker network rm assignment3-net
replicas=$(docker container ls -aq)
if [ -z "$replicas" ]; then
    exit 0
fi

docker container stop $replicas
docker container rm $replicas
