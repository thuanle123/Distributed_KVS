from collections import defaultdict
from flask import abort, Flask, request, jsonify, Response
from network import HTTPMethods, multicast
from time import sleep
import concurrent.futures
import heartbeat
import json
import os
import sys
import requests
import hashlib #to do hashing
import logging
import random

def int_sha256(key):
    key_bytestring = str(key).encode()
    hex_hash = hashlib.sha256(key_bytestring).hexdigest()
    return int(hex_hash, 16)

UNAUTHED_REPLICA_ORIGIN = {'error': 'Request must originate from a replica in the view'}
VIEW_PUT_SOCKET_EXISTS = {
    'error': 'Socket address already exists in the view',
    'message': 'Error in PUT'
}

app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)

store = {}
delivery_buffer = [] # Messages received but not yet delivered.
previously_received_vector_clocks = defaultdict(list)
previously_received_vector_clocks_CAPACITY = 1

replicas_view_universe = heartbeat.ADDRESSES
replicas_view_no_port = {x.split(":")[0] for x in replicas_view_universe}
replicas_view_universe_filename = heartbeat.UNIVERSE_FILENAME

my_address = heartbeat.MY_ADDRESS
my_address_no_port = my_address.split(":")[0]
replicas_view_alive = {my_address}
replicas_view_alive_filename = heartbeat.ALIVE_FILENAME
replicas_view_alive_last_read = float('-inf') # When was the last time we read from alive?  Initially never read.

vector_clock = {address: 0 for address in replicas_view_no_port}
vector_clock_filename = heartbeat.VECTOR_CLOCK_FILENAME

# Global shard variables loaded during startup().
SHARD_COUNT = None
shard_view_universe = None
shard_view_universe_no_port = None
shard_view_alive = None

def create_shard_view(replicas_view, num_shards):
    shard_view = [set() for _ in range(num_shards)] # List of SHARD_COUNT lists within it, where list i holds servers in ith shard.
    deterministic_view = sorted(replicas_view)
    for i, address in enumerate(deterministic_view):
        shard_view[i % num_shards].add(address)

    if not are_shards_fault_tolerant(shard_view):
        raise FaultToleranceError
    return shard_view

def are_shards_fault_tolerant(shard_view):
    return all([is_shard_fault_tolerant(s) for s in shard_view])

def is_shard_fault_tolerant(shard):
    MINIMUM_SERVERS_THRESHOLD = 2
    return len(shard) >= MINIMUM_SERVERS_THRESHOLD

class ShardError(Exception):
    pass

class FaultToleranceError(ShardError):
    pass

class NodeNotFoundError(ShardError):
    pass

class ShardNotFoundError(ShardError):
    pass

class ShardNoResponse(ShardError):
    pass


def get_my_id():
    shard_id = -1
    for i, shard in enumerate(shard_view_universe):
        if my_address in shard:
            shard_id = i
            break
    return shard_id

def startup():
    global SHARD_COUNT
    global shard_view_universe
    global shard_view_universe_no_port

    update_replicas_view_universe_file()
    update_vector_clock_file()

    add_replica_fs = broadcast_add_replica()

    try:
        SHARD_COUNT = int(os.environ['SHARD_COUNT'])
        shard_view_universe = create_shard_view(replicas_view_universe, SHARD_COUNT)
    except:
        # Pull shard state from another replica.
        pulled_shard_view = False
        for address in replicas_view_universe.difference({my_address}):
            response = requests.get('http://' + address + route_shard())
            if response.status_code == 200:
                shard_view_universe = [set(shard) for shard in response.json()['shard_view_universe']]
                SHARD_COUNT = len(shard_view_universe)
                pulled_shard_view = True
                break
        if not pulled_shard_view:
            raise ShardNoResponse

    shard_view_universe_no_port = [{x.split(":")[0] for x in shard} for shard in shard_view_universe]

    # Give us two pulses before we start doing anything.
    # First pulse to guarantee a heartbeat was attempted, second pulse for insurance.
    sleep(heartbeat.INTERVAL * 2)
    update_replicas_view_alive()

# add a node to shard
# add to a shard with a fewest nodes first
def add_to_shard():
    #position = 0
    #shards[position].append(ip)
    pass

# delete a node from a shard
def delete_from_shard():
    pass

# this only execute if CLIENT requests it
# IDEA: remove a shard from a max node and add it into a min node
def rebalance_shard():
    pass


def route(r=''):
    return '/key-value-store' + r

def route_shard(r=''):
    return '/key-value-store-shard' + r

@app.route(route('/union-store'))
def

@app.route(route_shard('/reshard'), methods=['PUT'])
def reshard():
    global shard_view_universe

    json_data = request.get_json()

    incoming_addr = request.remote_addr
    if incoming_addr not in replicas_view_universe_no_port: # From client.
        shard_count = json_data['shard-count']
        shard_view_universe = create_shard_view(replicas_view_universe, shard_count)
        multicast(
            replicas_view_universe,
            lambda a: 'http://' + a + route_shard('/reshard'),
            http_method=HTTPMethods.PUT,
            data=json.dumps({'shard_view_universe': [list(s) for s in shard_view_universe])})
            headers={'Content-Type': 'application/json'},
            timeout=3
        )
    else: # From replica.
        shard_view_universe = [set(s) for s in json_data['shard_view_universe']
    global SHARD_COUNT
    SHARD_COUNT = len(shard_view_universe)
    global vector_clock
    vector_clock = {address: 0 for address in replicas_view_no_port}

    # Copies all data in store to partitions to be sent out to other shards
    partitions = partition_store()

    # Clears its own delivery buffer and store
    global delivery_buffer
    delivery_buffer = []
    global store
    store = {}

    for shard_id, partition in partitions.items():
        multicast(
            shard_view_universe[shard_id],
            lambda a: 'http://' + a + route('/union-store'), # Create endpoint to extend store with another store in one request.
            http_method=HTTPMethods.PUT,
            data=json.dumps(partition),
            headers={'Content-Type': 'application/json'},
            timeout=3
        )

@app.route(route_shard(), methods=['GET'])
def shards_get():
    serializable = [list(shard) for shard in shard_view_universe]
    return jsonify({
        'shard_view_universe': serializable
    }), 200

@app.route(route_shard('/shard-ids'), methods=['GET'])
def shard_ids_get():
    shard_ids = sorted(range(len(shard_view_universe)))
    shard_ids_string = ','.join([str(i) for i in shard_ids])
    return jsonify({
        'message': 'Shard IDs retrieved successfully',
        'shard-ids': shard_ids_string
    }), 200

@app.route('/get_shard_view', methods=['GET'])
def tmp():
    update_replicas_view_alive()
    svu_serializable = [list(shard) for shard in shard_view_universe]
    sva_serializable = [list(shard) for shard in shard_view_alive]
    return jsonify({
        'replicas_view_universe': list(replicas_view_universe),
        'replicas_view_alive': list(replicas_view_alive),
        'shard_view_universe': svu_serializable,
        'shard_view_alive': sva_serializable,
    })


# Get the shard ID of a node
@app.route(route_shard('/node-shard-id'), methods=['GET'])
def node_id_get():
    shard_id = get_my_id()

    if shard_id == -1:
        raise NodeNotFoundError

    return jsonify({
        'message': 'Shard ID of the node retrieved successfully',
        'shard-id': str(shard_id)
    }), 200

# Get the members of a shard ID
@app.route(route_shard('/shard-id-members/<shard_id>'), methods=['GET'])
def members_id_get(shard_id):
    try:
        shard_id = int(shard_id)
        shard = sorted(shard_view_universe[shard_id])
        members = ','.join(shard)
    except (ValueError, IndexError):
        raise ShardNotFoundError

    return jsonify({
        'message': 'Members of shard ID retrieved successfully',
        'shard-id-members': members
    }), 200

# Get the number of keys stored in a shard
@app.route(route_shard('/shard-id-key-count/<shard_id>'), methods=['GET'])
def shard_key_get(shard_id):
    shard_id = int(shard_id)
    shard = sorted(shard_view_universe[shard_id])
    server = random.randrange(len(shard))
    response = requests.get('http://' + shard[server] + route())
    if response.status_code == 200:
        store_with_deliveries = response.json()
        store_ = store_with_deliveries['store']
        return jsonify({
            'message': 'Key count of shard ID retrieved successfully',
            'shard-id-key-count': len(store_.keys())
        }), 200
    raise ShardNoResponse


@app.route(route_shard('/add-member/<shard_id>'), methods=['PUT'])
def shard_add_member(shard_id):
    shard_id = int(shard_id)

    json_data = request.get_json()
    new_member = json_data['socket-address']

    if new_member not in replicas_view_universe:
        return jsonify({
            'message': "Node does not exist in our view, can't add to shard"
        }), 400

    if any([new_member in shard for shard in shard_view_universe]): # Is the new member in some shard?
        if new_member in shard_view_universe[shard_id]:
            return jsonify({
                'message': f'Discarded: {new_member} is already a member of shard {shard_id}'
            }), 200
        else:
            return jsonify({
                'message': f'Discarded: {new_member} is already a member of another shard'
            }), 201

    shard_view_universe[shard_id].add(new_member)

    # Forward this request to everyone.
    multicast(
        replicas_view_universe,
        lambda address: 'http://' + address + route_shard(f'/add-member/{shard_id}'),
        http_method=HTTPMethods.PUT,
        timeout=3,
        data=request.get_data(),
        headers=request.headers,
    )

    return jsonify({
        'message': f'Successfully added node {new_member} to shard {shard_id}',
    }), 200

############# Old Stuff#############3
def pull_state(ip):
    global vector_clock
    global delivery_buffer
    global store

    response = requests.get('http://' + ip + route(), timeout=3)
    if response is None and response.status_code == 200:
        store_with_deliveries = response.json()
        store = store_with_deliveries['store']
        delivery_buffer = store_with_deliveries['delivery_buffer']
        vector_clock = store_with_deliveries['vector_clock']
        return

def broadcast_add_replica():
    return multicast(
        replicas_view_universe,
        lambda address: 'http://' + address + route('-view'),
        http_method=HTTPMethods.PUT,
        timeout=3,
        data=json.dumps({'socket-address': my_address}),
        headers={'Content-Type': 'application/json'}
    )


def is_replica(ip):
    replica_ips = {address.split(':')[0] for address in replicas_view_universe}
    return ip in replica_ips


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

def update_replicas_view_universe_file():
    with open(replicas_view_universe_filename, 'w') as f:
        json.dump(sorted(replicas_view_universe), f)

def update_vector_clock_file():
    with open(vector_clock_filename, 'w') as f:
        json.dump(vector_clock, f)

def update_shard_view_alive():
    global shard_view_alive
    shard_view_alive = [{x for x in shard if x in replicas_view_alive} for shard in shard_view_universe]

def update_replicas_view_alive():
    global replicas_view_alive
    try:
        modified_since_read = os.path.getmtime(replicas_view_alive_filename) >= replicas_view_alive_last_read
        if not modified_since_read:
            return
        with open(replicas_view_alive_filename, 'r') as f:
            replicas_view_alive = set(json.load(f))
    except (FileNotFoundError, IOError):
        replicas_view_alive = set()
    replicas_view_alive.add(my_address)
    update_shard_view_alive()


def add_replica_alive(new_address):
    global replicas_view_alive
    try:
        with open(replicas_view_alive_filename, 'r+') as f:
            replicas_view_alive = set(json.load(f)) # Update current alive set.
            replicas_view_alive.add(my_address)
            replicas_view_alive.add(new_address)

            f.truncate(0)
            f.seek(0)
            json.dump(list(replicas_view_alive), f) # Add replica.
    except (FileNotFoundError, IOError):
        with open(replicas_view_alive_filename, 'w') as f:
            replicas_view_alive = {my_address, new_address}
            json.dump(list(replicas_view_alive), f)


def send_update_put(key, message):
    vector_clock[my_address_no_port] += 1
    update_vector_clock_file()
    headers = {'VC': json.dumps(vector_clock), 'Content-Type': 'application/json'}

    my_id = get_my_id()
    if my_id == -1:
        return

    multicast(
            shard_view_universe[my_id],
            lambda address: 'http://' + address + route('/' + key),
            http_method=HTTPMethods.PUT,
            timeout=3,
            data=json.dumps(message),
            headers=headers
            )


def send_update_delete(key):
    vector_clock[my_address_no_port] += 1
    update_vector_clock_file()
    headers = {'VC': json.dumps(vector_clock), 'Content-Type': 'application/json'}

    my_id = get_my_id()
    if my_id == -1:
        return

    multicast(
            shard_view_universe[my_id],
            lambda address: 'http://' + address + route('/' + key),
            http_method=HTTPMethods.DELETE,
            timeout=3,
            headers=headers
    )


def format_response(message, does_exist=None, error=None, value=None, replaced=None):
    metadata = json.dumps(vector_clock, sort_keys=True)
    res = {'message': message, 'causal-metadata' : metadata, 'version' : metadata, 'shard-id': str(get_my_id())}

    if does_exist is not None:
        res['doesExist'] = does_exist

    if error is not None:
        res['error'] = error

    if value is not None:
        res['value'] = value

    if replaced is not None:
        res['replaced'] = replaced

    return jsonify(res)

def can_be_delivered_client(incoming_vec):
    update_replicas_view_alive()

    my_id = get_my_id()
    if my_id == -1:
        return False

    for x in shard_view_alive[my_id]:
        cur_addr = x.split(":")[0]
        if vector_clock[cur_addr] < incoming_vec[cur_addr]:
            return False
    return True

def can_be_delivered(incoming_vec, incoming_addr):
    update_replicas_view_alive()

    my_id = get_my_id()
    if my_id == -1:
        return False

    for x in shard_view_alive[my_id]:
        cur_addr = x.split(":")[0]
        if cur_addr == incoming_addr:
            continue
        if vector_clock[cur_addr] < incoming_vec[cur_addr]:
            return False
    if incoming_vec[incoming_addr] == vector_clock[incoming_addr] + 1:
        return True
    return False

def attempt_deliver_put_message(key, json_data):
    if 'value' not in json_data:
        return format_response('Error in PUT', error='Value is missing'), 400
    value = json_data['value']

    key_exists = key in store
    if not key_exists and len(key) > 50:
        return format_response('Error in PUT', error='Key is too long'), 400
    store[key] = value

    if key_exists:
        return format_response('Updated successfully', replaced=True), 200
    return format_response('Added successfully', replaced=False), 201

def attempt_deliver_delete_message(key):
    if key in store:
        del store[key]
        return format_response('Deleted successfully', does_exist=True), 200
    return format_response('Error in DELETE', does_exist=False, error='Key does not exist'), 404

def deliver_from_buffer():
    for item in delivery_buffer:
        incoming_vec = item[0]
        meta_data = item[1]
        incoming_addr = meta_data[1]
        if can_be_delivered(incoming_vec, incoming_addr):
            # deliver message here and remove from buffer
            if meta_data[0] == 'PUT':
                attempt_deliver_put_message(meta_data[2], meta_data[3])
                delivery_buffer.remove(item)
                vector_clock[incoming_addr] += 1
                update_vector_clock_file()
            elif meta_data[0] == 'DELETE':
                attempt_deliver_delete_message(meta_data[2])
                delivery_buffer.remove(item)
                vector_clock[incoming_addr] += 1
                update_vector_clock_file()
            # call deliver_from_buffer() again
            deliver_from_buffer()
            return

@app.route(route(), methods=['GET'])
def store_get():
    resp = {'store': store, 'delivery_buffer': delivery_buffer, 'vector_clock': vector_clock}
    return jsonify(resp), 200

@app.route(route('/<key>'), methods=['GET'])
def kvs_get(key):
    update_replicas_view_alive()

    hashed_id = int_sha256(key) % SHARD_COUNT
    if hashed_id != get_my_id():
        shard = sorted(shard_view_alive[hashed_id])
        if (len(shard) == 0):
            return '', 418
        server = random.randrange(len(shard))
        response = requests.get('http://' + shard[server] + route('/' + key))
        return (response.text, response.status_code, response.headers.items())

    if key in store:
        return format_response('Retrieved successfully', does_exist=True, value=store[key]), 200
    return format_response('Error in GET', error='Key does not exist', does_exist=False), 404


@app.route(route('/<key>'), methods=['PUT'])
def kvs_put(key):
    json_data = request.get_json()
    incoming_addr = request.remote_addr

    update_replicas_view_alive()

    hashed_id = int_sha256(key) % SHARD_COUNT
    my_id = get_my_id()
    if hashed_id != my_id:
        shard = sorted(shard_view_alive[hashed_id])
        if (len(shard) == 0):
            return '', 418
        server = random.randrange(len(shard))
        response = requests.put('http://' + shard[server] + route('/' + key), headers=request.headers, data=request.get_data())
        return (response.text, response.status_code, response.headers.items())

    # Check here if message from fellow servers
    if incoming_addr == my_address_no_port:
        # Ignore messages sent from itself
        return format_response('Discarded'), 200
    elif incoming_addr not in shard_view_universe_no_port[my_id]:
        # Check for causal metadata
        has_metadata = json_data is not None and 'causal-metadata' in json_data and json_data['causal-metadata'] != ''
        if has_metadata:
            while not can_be_delivered_client(json.loads(json_data['causal-metadata'])):
                sleep(5)
        # Forward message here
        send_update_put(key, json_data)
        return attempt_deliver_put_message(key, json_data)
    else:
        # Check vector clock here. If out of order, cache
        incoming_vec = json.loads(request.headers.get('VC'))
        if can_be_delivered(incoming_vec, incoming_addr):
            # deliver message
            vector_clock[incoming_addr] += 1
            update_vector_clock_file()
            out = attempt_deliver_put_message(key, json_data)
            # deliver all messages in buffer
            deliver_from_buffer()
            return out
        else:
            delivery_buffer.append([incoming_vec, ['PUT', incoming_addr, key, json_data]])
            return format_response('Cached successfully'), 200

@app.route(route('/<key>'), methods=['DELETE'])
def kvs_delete(key):
    update_replicas_view_alive()

    hashed_id = int_sha256(key) % SHARD_COUNT
    my_id = get_my_id()
    if hashed_id != my_id:
        shard = sorted(shard_view_alive[hashed_id])
        if (len(shard) == 0):
            return '', 418
        server = random.randrange(len(shard))
        response = requests.delete('http://' + shard[server] + route('/' + key), headers=request.headers, data=request.get_data())
        return (response.text, response.status_code, response.headers.items())

    # Check here if message from fellow server
    incoming_addr = request.remote_addr
    json_data = request.get_json()
    if incoming_addr == my_address_no_port:
        # Ignore messages sent from itself
        return format_response('Discarded'), 200
    elif incoming_addr not in shard_view_universe_no_port[my_id]: # Not from my shard.
        # Check for causal metadata
        has_metadata = json_data is not None and 'causal-metadata' in json_data and json_data['causal-metadata'] != ''
        if has_metadata:
            while not can_be_delivered_client(json.loads(json_data['causal-metadata'])):
                sleep(5)
        # Forward message here
        send_update_delete(key)
        return attempt_deliver_delete_message(key)
    else:
        # Check vector clock here. If out of order, cache
        incoming_vec = json.loads(request.headers.get('VC'))
        if can_be_delivered(incoming_vec, incoming_addr):
            # deliver message
            vector_clock[incoming_addr] += 1
            update_vector_clock_file()
            out = attempt_deliver_delete_message(key)
            # deliver all messages in buffer
            deliver_from_buffer()
            return out
        else:
            delivery_buffer.append([incoming_vec, ['DELETE', incoming_addr, key]])
            return format_response('Cached successfully'), 200

@app.route(route('-view'), methods=['GET'])
def view_get():
    #if not is_replica(request.remote_addr):
    #   return jsonify(UNAUTHED_REPLICA_ORIGIN), 401

    update_replicas_view_alive()
    return jsonify({
        'message': 'View retrieved successfully',
        'view': ",".join(str(x) for x in sorted(replicas_view_alive))
    }), 200


@app.route(route('-view'), methods=['PUT'])
def view_put():
    #if not is_replica(request.remote_addr):
    #    return jsonify(UNAUTHED_REPLICA_ORIGIN), 401

    request_body = request.get_json()
    target = request_body['socket-address']

    update_replicas_view_alive()

    if target in replicas_view_alive:
        return jsonify(VIEW_PUT_SOCKET_EXISTS), 404
    else:
        if target not in replicas_view_universe:
            target_no_port = target.split(':')[0]
            vector_clock[target_no_port] = 0
            update_vector_clock_file()
            replicas_view_no_port.add(target_no_port)
            replicas_view_universe.add(target)
            update_replicas_view_universe_file()

        replicas_view_alive.add(target)

        with open(replicas_view_alive_filename, 'r+') as f:
            previous_alive = set(json.load(f))
            if target not in previous_alive:
                current_alive = previous_alive.union({target})
                f.truncate(0)
                f.seek(0)
                json.dump(list(current_alive), f)

        return jsonify({
            'message': 'Replica added successfully to the view'}
        ), 200


@app.route(route('-view'), methods=['DELETE'])
def view_delete():
    #if not is_replica(request.remote_addr):
    #    return jsonify(UNAUTHED_REPLICA_ORIGIN), 401

    request_body = request.get_json()
    target = request_body['socket-address']

    update_replicas_view_alive()
    if target in replicas_view_alive:
        replicas_view_alive.remove(target)
        return jsonify({
            'message': 'Replica deleted successfully from the view'}
        ), 200
    else:
        return jsonify({
            'error': 'Socket address does not exist in the view',
            'message': 'Error in DELETE'
        }), 404


@app.route(heartbeat.ENDPOINT, methods=['GET'])
def heartbeat_get():
    incoming_vec = json.loads(request.headers.get('VC'))
    incoming_addr = request.remote_addr

    incoming_addr_with_port = None
    for addr_with_port in replicas_view_universe:
        addr = addr_with_port.split(':')[0]
        if addr == incoming_addr:
            incoming_addr_with_port = addr_with_port

    sender_is_replica = incoming_addr_with_port is not None
    if not sender_is_replica:
        return jsonify({'status': 'OK'}), 200

    previous_vecs = previously_received_vector_clocks[incoming_addr]
    steady_state = all([incoming_vec == previous_vec for previous_vec in previous_vecs])
    my_id = get_my_id()
    if steady_state and my_id != -1 and incoming_addr in shard_view_universe[my_id]:
        if not can_be_delivered_client(incoming_vec):
            pull_state(incoming_addr_with_port)

    # Double ended queue.  Dequeue last VC from the beginning if at capacity.  Enqueue incoming VC to the end.
    if (len(previously_received_vector_clocks[incoming_addr]) > previously_received_vector_clocks_CAPACITY):
        del previously_received_vector_clocks[incoming_addr][0]
    previously_received_vector_clocks[incoming_addr].append(incoming_vec)

    return jsonify({'status': 'OK'}), 200


if __name__ == '__main__':
    startup()
    app.run(host='0.0.0.0', port=determine_port())
