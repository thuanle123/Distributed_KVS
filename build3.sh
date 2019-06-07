#!/bin/bash
set -x

./clean.sh
docker network create --subnet=10.10.0.0/16 assignment4-net
docker build -t assignment4-image .

export VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080"

#Run containers in detached mode
docker run -d -p 8082:8080 --net=assignment4-net --ip=10.10.0.2 --name="node1" -e SOCKET_ADDRESS="10.10.0.2:8080" -e VIEW=$VIEW -e SHARD_COUNT="1" assignment4-image
docker run -d -p 8083:8080 --net=assignment4-net --ip=10.10.0.3 --name="node2" -e SOCKET_ADDRESS="10.10.0.3:8080" -e VIEW=$VIEW -e SHARD_COUNT="1" assignment4-image
docker run -d -p 8084:8080 --net=assignment4-net --ip=10.10.0.4 --name="node3" -e SOCKET_ADDRESS="10.10.0.4:8080" -e VIEW=$VIEW -e SHARD_COUNT="1" assignment4-image
