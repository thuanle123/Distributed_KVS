This collaborative project is written by Allston Mickey, Kai Ergin, Kyle O'Brien
and Thuan Le

# Mechanisms

1. We use heartbeat protocol to detect when a replica is down. The heartbeat will
run at a regular interval every 2.5 seconds with a time out of 2 seconds until
heartbeat failure. Our heartbeat uses unicast and multicast to check for alive
replicas.
2. For casual consistency tracking, we use vector clock to ensure it. We use
delivery buffer to put out of order message in a buffer, and wait for the right
vector clock messages to arrived.
3. For implementing sharding, we used a round robin method to assign servers to
shards. We then hash any key received and mod it to our shard number to
determine the shard that the key belongs in. The message is then sent to a
random server in the corresponding shard. The retrieving server in the shard
will update all the other servers in its own shard. Vector clocks are only
consistent between shards.
4. For resharding, there are two stages. The first server to receive a reshard
request will calculate the new shard view based on the shard count provided.
The reshard request along with the new shard view will then be forwarded
to all other servers. The second stage involves copying and clearing the
entire KVS and rehashing all the keys. All keys are then sent to their new
shard.

# Acknowledgment

* We all met up together and worked out the concepts in general, and with regards to the project specs.
* We did pair programming and developed alongside each other.
* Special thanks Professor Lindsey Kuper and TA Reza Nasirigerdeh
