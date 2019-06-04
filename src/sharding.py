# Package and Lib Imports
import os
from flask import app, abort, Flask, request, jsonify, Response

# add a node to shard
# add to a shard with a fewest nodes first


SHARD_COUNT = int(os.getenv('SHARD_COUNT'))
shards = {[] for _ in range(SHARD_COUNT)}  # I want this to be a dictionary
shard_id = None


def add_to_shard():
    # position = 0
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
    # member_id = int(shard_id)
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
