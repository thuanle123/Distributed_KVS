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

2. /key-value-store-view
   - Support GET, PUT, DELETE request

3. /key-value-store/<key>
   - Support GET, PUT and DELETE request
