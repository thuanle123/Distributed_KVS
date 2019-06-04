# Package and Lib Imports
from collections import defaultdict
from flask import abort, Flask, request, jsonify, Response
from network import HTTPMethods, multicast
import concurrent.futures
import heartbeat
import json
import os
import sys
import requests
import hashlib  # to do hashing

# Project Level Imports
from src.routes import route, route_shard
from src.view import VIEW_PUT_SOCKET_EXISTS, update_replicas_view_alive, pull_state


app = Flask(__name__)

store = {}
delivery_buffer = []  # Messages received but not yet delivered.
previously_received_vector_clocks = defaultdict(list)
previously_received_vector_clocks_CAPACITY = 1

replicas_view_universe = heartbeat.ADDRESSES
replicas_view_no_port = [x.split(":")[0] for x in replicas_view_universe]
vector_clock = {address: 0 for address in replicas_view_no_port}

my_address = heartbeat.MY_ADDRESS
my_address_no_port = my_address.split(":")[0]
replicas_view_alive = {my_address}
replicas_view_alive_filename = heartbeat.FILENAME
# When was the last time we read from alive?  Initially never read.
replicas_view_alive_last_read = float('-inf')

SHARD_COUNT = int(os.getenv('SHARD_COUNT'))
shards = {[] for _ in range(SHARD_COUNT)}  # I want this to be a dictionary
shard_id = None


def startup():
    update_vector_clock_file()

    add_replica_fs = broadcast_add_replica()

    def we_exist_in_view(unicast_response):
        http_response = unicast_response.response
        if http_response is None:
            return False
        added_to_view = http_response.status_code == 200
        preexisted = http_response.status_code == 401 and http_response.json(
        ) == VIEW_PUT_SOCKET_EXISTS
        return added_to_view or preexisted

    # Get alive replicas from our broadcast.
    unicast_responses = [f.result()
                         for f in concurrent.futures.as_completed(add_replica_fs)]
    unicast_responses_existing = filter(we_exist_in_view, unicast_responses)

    global replicas_view_alive
    replicas_view_alive = set(
        map(lambda ur: ur.address, unicast_responses_existing))
    replicas_view_alive.add(my_address)
    update_replicas_view_alive()
    pull_state()

# add a node to shard
# add to a shard with a fewest nodes first


def add_to_shard():
    #position = 0
    # shards[position].append(ip)
    pass

# delete a node from a shard


def delete_from_shard():
    pass

# this only execute if CLIENT requests it
# IDEA: remove a shard from a max node and add it into a min node


def rebalance_shard():
    pass

# Add all the route here so we don't have to scroll
# move it down when we are done
# 4 GET requests from the PDF
# Please double check it too

# TODO
# Get the shard IDs of the store
# Format is "shard-ids":"1,2"
@app.route(route_shard('/shard-ids'), methods=['GET'])
def shard_id_get():
    ids = "0"
    for i in range(1, len(Shards)):
        ids += ',' + str(i)
    return jsonify({
        'message': 'Shard IDs retrieved succesfully',
        'shard-ids': ids
    }), 200
    # pass

# Get the shard IDs of a node
@app.route(route_shard('/node-shard-id'), methods=['GET'])
def node_id_get():
    # Need to initialize the node onto the shard first
    return jsonify({
        'message': 'Shard ID of the node retrieved successfully',
        'shard-id': shard_id
    })
    # pass

# Get the members of a shard ID
# @app.route(route_shard('/shard-id-members/<shard-id>'), methods=['GET'])


def member_id_get(member_id):
    #member_id = int(shard_id)
    # http://<node-socket-address>/key-value-store-shard/shard-id-members/<shard-id>
    # {"message":"Members of shard ID retrieved successfully", "shard-id-members":<shard-id-members>} 200
    # return jsonify({
    #   'message':'Members of shard ID retrieved successfully',
    #   'shard-id-members': #VIEW of the -view
    #   })
    pass

# Get the number of keys stored in a shard
# @app.route(route_shard('/shard-id-key-count/<shard-id>'), methods=['GET'])


def key_get():
    # http://<node-socket-address>/key-value-store-shard/shard-id-key-count/<shard-id>
    # {"message":"Key count of shard ID retrieved successfully", "shard-id-key-count":<shard-id-key-count>} 200
    pass

# Old Stuff#############3


def broadcast_add_replica():
    return multicast(
        replicas_view_universe,
        lambda address: 'http://' + address + route('-view'),
        http_method=HTTPMethods.PUT,
        timeout=1,
        data=json.dumps({'socket-address': my_address}),
        headers={'Content-Type': 'application/json'}
    )


def determine_port():
    """
    Parses the SOCKET_ADDRESS env variable for the port on which the
    server should run on. If not found, the server runs on port 8080.

    Returns:
        int: The port to run on.
    """
    try:
        port = int(my_address.split(':')[1])
    except Exception:
        print('Socket Error: Unable to parse port from socket argument.')
        print('Defaulting to port 8080...')
        port = 8080

    return port


def update_vector_clock_file():
    with open('.vector_clock.json', 'w') as f:
        json.dump(vector_clock, f)


if __name__ == '__main__':
    startup()
    app.run(host='0.0.0.0', port=determine_port())
