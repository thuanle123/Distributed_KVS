#!/bin/bash
set -x

./clean.sh
docker network create --subnet=10.10.0.0/16 mynet
docker build -t assignment4-image .

export VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080,10.10.0.5:8080,10.10.0.6:8080,10.10.0.7:8080"

#Run containers in detached mode
docker run -d -p 8082:8080 --net=mynet --ip=10.10.0.2 --name="node1" -e SOCKET_ADDRESS="10.10.0.2:8080" -e VIEW=$VIEW -e SHARD_COUNT="2" assignment4-image
docker run -d -p 8083:8080 --net=mynet --ip=10.10.0.3 --name="node2" -e SOCKET_ADDRESS="10.10.0.3:8080" -e VIEW=$VIEW -e SHARD_COUNT="2" assignment4-image
docker run -d -p 8084:8080 --net=mynet --ip=10.10.0.4 --name="node3" -e SOCKET_ADDRESS="10.10.0.4:8080" -e VIEW=$VIEW -e SHARD_COUNT="2" assignment4-image
docker run -d -p 8085:8080 --net=mynet --ip=10.10.0.5 --name="node4" -e SOCKET_ADDRESS="10.10.0.5:8080" -e VIEW=$VIEW -e SHARD_COUNT="2" assignment4-image
docker run -d -p 8086:8080 --net=mynet --ip=10.10.0.6 --name="node5" -e SOCKET_ADDRESS="10.10.0.6:8080" -e VIEW=$VIEW -e SHARD_COUNT="2" assignment4-image
docker run -d -p 8087:8080 --net=mynet --ip=10.10.0.7 --name="node6" -e SOCKET_ADDRESS="10.10.0.7:8080" -e VIEW=$VIEW -e SHARD_COUNT="2" assignment4-image
