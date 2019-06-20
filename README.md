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
       - /key-value-store-shard/shard-id-members/\<shard_id>
       - /key-value-store-shard/shard-id-key-count/\<shard_id>
     - PUT request
       - /key-value-store-shard/add-member/\<shard_id>
       - /key-value-store-shard/reshard

2. **/key-value-store-view**
     - Support GET, and DELETE request
     - PUT request is not supported because eventually a new node will get added into the view

3. **/key-value-store/\<key>**
     - Support GET, PUT and DELETE request

# Setup
You will need to install docker to be able to run this project

[On Window](https://docs.docker.com/docker-for-windows/install/)

[On Mac](https://docs.docker.com/docker-for-mac/install/)

[On Linux](https://linuxize.com/post/how-to-install-and-use-docker-on-ubuntu-18-04/)

Then run this command to see if it is installed successfully
```
docker -v
Output
Docker version 18.09.6, build 481bc77
```
Run this script to build and run 6 containers with 2 initial shards
This script will also create a subnet as well as a Docker image
```
chmod +x build.sh
./build.sh
```

# Usage
* You can now send GET, PUT, DELETE requests to all the endpoints above

## 1. **GET shard IDs of a store**
Say we have 7 nodes and 3 shards
```
curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://localhost:8082/key-value-store-shard/shard-ids
```
**Response**
```
{"message":"Shard IDs retrieved successfully","shard-ids":"0,1,2"}
200
```
## 2. **GET shard ID of a node**
Say we have 7 nodes with 3 shards
```
curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://localhost:8082/key-value-store-shard/node-shard-id
```
**Response**
```
{"message":"Shard ID of the node retrieved successfully","shard-id":"0"}
200
```
## 3. **GET shard ID members**
Say we have 7 nodes with 3 shards, after running consistent hashing we might have
```
curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://localhost:8082/key-value-store-shard/shard-id-members/0
```
**Response**
```
{"message":"Members of shard ID retrieved successfully","shard-id-members":"10.10.0.2:8080,10.10.0.5:8080,10.10.0.8:8080"}
200
```
## 4. **GET key count in a shard**
Say we have 7 nodes with 3 shards and 600 keys, in which shard 0 contains 204 keys, shard 1 contains 182 keys and shard 2 contains 214 keys
```
curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://localhost:8082/key-value-store-shard/shard-id-key-count/0
```
**Response**
```
{"message":"Key count of shard ID retrieved successfully","shard-id-key-count":204}
200
```
## 5. **Add a new node into a shard**
Add a new node node8 with socket address 10.10.0.9:8080 to shard 2

Start a container without the SHARD_COUNT environment variable

Then send a PUT request
```
docker run -d -p 8089:8080 --net=mynet --ip=10.10.0.9 --name="node8" -e SOCKET_ADDRESS="10.10.0.9:8080" -e VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080,10.10.0.5:8080,10.10.0.6:8080,10.10.0.7:8080,10.10.0.8:8080,10.10.0.9:8080" kvs-image
```
```
curl --request PUT --header "Content-Type: application/json" --write-out "%{http_code}\n" --data '{"socket-address": "10.10.0.9:8080"}' http://localhost:8082/key-value-store-shard/add-member/2
```
## 6. **Reshard a key-value store**
Say we have 6 nodes and 2 shards with 4 nodes on 1 shard and 2 nodes on the other
A node on shard 2 fails, so the client send a reshard request with a shard count of 2
```
curl --request PUT --header "Content-Type: application/json" --write-out "%{http_code}\n" --data '{"shard-count": 2 }' http://localhost:8082/key-value-store-shard/reshard
```
**Response**
```
{"message":"Resharding done successfully"}
200
```
**Invalid resharding (5 nodes with 3 shard counts), send error**
```
curl --request PUT --header "Content-Type: application/json" --write-out "%{http_code}\n" --data '{"shard-count": 3 }' http://localhost:8082/key-value-store-shard/reshard
```
**Response**
```
{"message":"Not enough nodes to provide fault-tolerance with the given shard count!"}
400
```
## 7. **GET a replica's view of the store**
Say we have 7 nodes up, we can check its view
```
curl --request GET --header "Content-Type: application/json" --write-out
"%{http_code}\n" http://localhost:8082/key-value-store-view
```
**Response**
```
{"message":"View retrieved successfully","view":"10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080,10.10.0.5:8080,10.10.0.6:8080,10.10.0.7:8080,10.10.0.8:8080"}
200
```
## 8. **DELETE a replica's view of the store**
```
curl --request DELETE --header "Content-Type: application/json" --write-out "%{http_code}\n" --data '{"socket-address": "10.10.0.4:8080" }' http://localhost:8082/key-value-store-view
```
**Response**
```
{"message":"Replica deleted successfully from the view"}
200
```
**Error Response if the replica not in the VIEW**
```
{"error":"Socket address does not exist in the view","message":"Error in DELETE"}
404
```
## 9. **PUT a key into the store**
```
curl --request PUT --header "Content-Type: application/json" --write-out "%{http_code}\n" --data '{"value": "", "causal-metadata": ""}' http://localhost:8082/key-value-store/key1
```
**Response**
```
{"causal-metadata":"{\"10.10.0.2\": 1, \"10.10.0.3\": 0, \"10.10.0.4\": 0, \"10.10.0.5\": 0, \"10.10.0.6\": 0, \"10.10.0.7\": 0}","message":"Added successfully","replaced":false,"shard-id":"0","version":"{\"10.10.0.2\": 1, \"10.10.0.3\": 0, \"10.10.0.4\": 0, \"10.10.0.5\": 0, \"10.10.0.6\": 0, \"10.10.0.7\": 0}"}
201
```
## 10. **GET a key from the store**
```
curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://localhost:8082/key-value-store/key1
```
**Response**
```
{"causal-metadata":"{\"10.10.0.2\": 2, \"10.10.0.3\": 0, \"10.10.0.4\": 0, \"10.10.0.5\": 0, \"10.10.0.6\": 0, \"10.10.0.7\": 0}","doesExist":true,"message":"Retrieved successfully","shard-id":"0","value":"","version":"{\"10.10.0.2\": 2, \"10.10.0.3\": 0, \"10.10.0.4\": 0, \"10.10.0.5\": 0, \"10.10.0.6\": 0, \"10.10.0.7\": 0}"}
200
```
## 11. **DELETE a key from the store**
```
curl --request DELETE --header "Content-Type: application/json" --write-out "%{http_code}\n" --data '{"causal-metadata": ""}' http://localhost:8082/key-value-store/key1
```
**Response**
```
{"causal-metadata":"{\"10.10.0.2\": 3, \"10.10.0.3\": 0, \"10.10.0.4\": 0, \"10.10.0.5\": 0, \"10.10.0.6\": 0, \"10.10.0.7\": 0}","doesExist":true,"message":"Deleted successfully","shard-id":"0","version":"{\"10.10.0.2\": 3, \"10.10.0.3\": 0, \"10.10.0.4\": 0, \"10.10.0.5\": 0, \"10.10.0.6\": 0, \"10.10.0.7\": 0}"}
200
```
The causal-metadata holds the vector clock of a particular server.
# Removal

* The following command will remove all the subnet, as well as stopping and
removing all containers
```
chmod +x clean.sh
./clean.sh
```

