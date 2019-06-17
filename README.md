# Sharded fault-tolerant distributed key-value store
A sharded, fault-tolerant key-value store with a better fault tolerance, capacity,
and throughput. This project is written in python using Flask framework and
being deploy in Docker. We used heartbeat protocol, consistent hashing
algorithm, causal consistency, eventual consistency and broadcast protocol to
build this project.

# Endpoints support

1. **/key-value-store-shard**
     - GET request
       - /key-value-store-shard/node-shard-id
       - /key-value-store-shard/shard-id-members/<shard-id>
       - /key-value-store-shard/shard-id-key-count/<shard-id>
     - PUT request
       - /key-value-store-shard/add-member/<shard-id>
       - /key-value-store-shard/reshard

2. **/key-value-store-view**
     - Support GET, PUT, DELETE request

3. **/key-value-store/<key>**
     - Support GET, PUT and DELETE request

# Setup

* You will need to install docker to be able to run this project
* [On Window](https://docs.docker.com/docker-for-windows/install/)
* [On Mac](https://docs.docker.com/docker-for-mac/install/)
* [On Linux](https://linuxize.com/post/how-to-install-and-use-docker-on-ubuntu-18-04/)
* Then run this command to see if it is installed successfully
```
docker -v
Output
Docker version 18.09.6, build 481bc77
```
* Run this script to build and run 6 containers with 2 initial shards
* This script will also create a subnet as well as a Docker image
```
chmod +x build.sh
./build.sh
```

# Usage
* You can now send GET, PUT, DELETE requests to all the endpoints above

1. GET shard IDs of a store
   - Say we have 7 nodes and 3 shards
```
curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://localhost:8082/key-value-store-shard/shard-ids
```
Response
```
{"message":"Shard IDs retrieved successfully","shard-ids":"0,1,2"} 200
```
2. GET shard ID of a node
   - Say we have 7 nodes with 3 shards
```
curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://10.10.0.2:8082/key-value-store-shard/node-shard-id
```
Response
```
{"message":"Shard ID of the node retrieved successfully","shard-id":"0"}
```
3. GET shard ID members
   - Say we have 7 nodes with 3 shards, after running consistent hashing we might have
```
curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://10.10.0.2:8082/key-value-store-shard/shard-id-members/0
```
Response
```
{"message":"Members of shard ID retrieved successfully","shard-id-members":"10.10.0.2:8080,10.10.0.5:8080,10.10.0.8:8080"}
```

# Removal

* The following command will remove all the subnet, as well as stopping and
removing all containers
```
chmod +x clean.sh
./clean.sh
```

