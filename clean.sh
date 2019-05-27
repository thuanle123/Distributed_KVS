#!/bin/bash

# Need to modify
set -x

replicas=$(docker container ls -aq)
if [ -z "$replicas" ]; then
    exit 0
fi

docker container stop $replicas
docker container rm $replicas
