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
replicas_view_no_port = [x.split(":")[0] for x in replicas_view_universe]

my_address = heartbeat.MY_ADDRESS
my_address_no_port = my_address.split(":")[0]
replicas_view_alive = {my_address}
replicas_view_alive_filename = heartbeat.FILENAME
replicas_view_alive_last_read = float('-inf') # When was the last time we read from alive?  Initially never read.

vector_clock = {address: 0 for address in replicas_view_no_port}

def create_shard_view(replicas_view, num_shards):
    shard_view = [[] for _ in range(num_shards)] # List of SHARD_COUNT lists within it, where list i holds servers in ith shard.
    deterministic_view = sorted(list(replicas_view))
    for i, address in enumerate(deterministic_view):
        shard_view[i % num_shards].append(address)

    if not are_shards_fault_tolerant(shard_view):
        raise FaultToleranceError
    return shard_view

def are_shards_fault_tolerant(shard_view):
    return all([is_shard_fault_tolerant(s) for s in shard_view])

def is_shard_fault_tolerant(shard):
    MINIMUM_SERVERS_THRESHOLD = 2
    return len(shard) >= MINIMUM_SERVERS_THRESHOLD

SHARD_COUNT = int(os.getenv('SHARD_COUNT'))
shard_view_universe = create_shard_view(replicas_view_universe, SHARD_COUNT)
shard_view_universe_no_port = [[x.split(":")[0] for x in shard] for shard in shard_view_universe]
shard_view_alive = [[x for x in shard if x in replicas_view_alive] for shard in shard_view_universe]

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


# it's aids that this has to be here. move later if necessary
def get_my_id():
    shard_id = -1
    for i, shard in enumerate(shard_view_universe):
        if my_address in shard:
            shard_id = i
            break
    return shard_id

def startup():
    update_vector_clock_file()

    add_replica_fs = broadcast_add_replica()

    def we_exist_in_view(unicast_response):
        http_response = unicast_response.response
        if http_response is None:
            return False
        added_to_view = http_response.status_code == 200
        preexisted = http_response.status_code == 401 and http_response.json() == VIEW_PUT_SOCKET_EXISTS
        return added_to_view or preexisted

    # Get alive replicas from our broadcast.
    unicast_responses = [f.result() for f in concurrent.futures.as_completed(add_replica_fs)]
    unicast_responses_existing = filter(we_exist_in_view, unicast_responses)

    global replicas_view_alive
    replicas_view_alive = set(map(lambda ur: ur.address, unicast_responses_existing))
    replicas_view_alive.add(my_address)
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

@app.route(route_shard('/shard-ids'), methods=['GET'])
def shard_ids_get():
    shard_ids = list(range(len(shard_view_universe)))
    shard_ids_string = ','.join(sorted([str(i) for i in shard_ids]))
    return jsonify({
        'message': 'Shard IDs retrieved successfully',
        'shard-ids': shard_ids_string
    }), 200

@app.route('/get_shard_view', methods=['GET'])
def tmp():
    return jsonify({
        'replicas_view_universe': list(replicas_view_universe),
        'replicas_view_alive': list(replicas_view_alive),
        'shard_view_universe': list(shard_view_universe),
        'shard_view_alive': list(shard_view_alive),
    })


# Get the shard ID of a node
@app.route(route_shard('/node-shard-id'), methods=['GET'])
def node_id_get():
    shard_id = get_my_id()

    if shard_id == -1:
        raise NodeNotFoundError

    return jsonify({
        'message': 'Shard ID of the node retrieved successfully',
        'shard-id': shard_id
    }), 200

# Get the members of a shard ID
@app.route(route_shard('/shard-id-members/<shard_id>'), methods=['GET'])
def members_id_get(shard_id):
    try:
        shard_id = int(shard_id)
        members = shard_view_universe[shard_id]
        members_string = ','.join(sorted(members))
    except (ValueError, IndexError):
        raise ShardNotFoundError

    return jsonify({
        'message': 'Members of shard ID retrieved successfully',
        'shard-id-members': members_string
    }), 200

# Get the number of keys stored in a shard
@app.route(route_shard('/shard-id-key-count/<shard_id>'), methods=['GET'])
def shard_key_get(shard_id):
    shard_id = int(shard_id)
    members = shard_view_universe[shard_id]
    for member in members:
        response = requests.get('http://' + member + route())
        if response.status_code == 200:
            store_with_deliveries = response.json()
            store = store_with_deliveries['store']
            return jsonify({
                'message': 'Key count of shard ID retrieved successfully',
                'shard-id-key-count': len(store.keys())
            }), 200
    raise ShardNoResponse

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

def update_vector_clock_file():
    with open('.vector_clock.json', 'w') as f:
        json.dump(vector_clock, f)

def update_shard_view_alive():
    global shard_view_alive
    shard_view_alive = [[x for x in shard if x in replicas_view_alive] for shard in shard_view_universe]

def update_replicas_view_alive():
    global replicas_view_alive
    try:
        modified_since_read = os.path.getmtime(replicas_view_alive_filename) >= replicas_view_alive_last_read
        if not modified_since_read:
            return

        old_view = replicas_view_alive
        with open(replicas_view_alive_filename, 'r') as f:
            replicas_view_alive = set(json.load(f))
            deleted_replicas = old_view.difference(replicas_view_alive)
            added_replicas = replicas_view_alive.difference(old_view)

        if len(deleted_replicas) > 0:
            # For each deleted replica,
            max_workers = min(len(deleted_replicas), 10)
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                for dr in deleted_replicas:
                    executor.submit(
                        unicast,
                        replicas_view_alive,
                        lambda address: 'http://' + address + route('-view'),
                        http_method=HTTPMethods.DELETE,
                        timeout=3,
                        data=json.dumps({'socket-address': dr}),
                        headers={'Content-Type': 'application/json'}
                    )

        if len(added_replicas) > 0:
            # For each deleted replica,
            max_workers = min(len(added_replicas), 10)
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                for ar in added_replicas:
                    executor.submit(
                        multicast,
                        replicas_view_alive,
                        lambda address: 'http://' + address + route('-view'),
                        http_method=HTTPMethods.PUT,
                        timeout=3,
                        data=json.dumps({'socket-address': ar}),
                        headers={'Content-Type': 'application/json'}
                    )

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
    res = {'message': message, 'causal-metadata' : metadata, 'version' : metadata}

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

    hashed_id = hash(key) % SHARD_COUNT
    if hashed_id != get_my_id():
        shard = shard_view_alive[hashed_id][:]
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
    app.logger.debug(f'==== entered kvs_put({key})')
    app.logger.debug(f'(before) replicas_view_alive: {replicas_view_alive}')
    app.logger.debug(f'(before) shard_view_alive: {shard_view_alive}')
    update_replicas_view_alive()
    app.logger.debug(f'(after) replicas_view_alive: {replicas_view_alive}')
    app.logger.debug(f'(after) shard_view_alive: {shard_view_alive}')
    app.logger.debug(f'shard_view_universe: {shard_view_universe}')

    hashed_id = hash(key) % SHARD_COUNT
    my_id = get_my_id()
    app.logger.debug(f'hashed_id: {hashed_id}')
    app.logger.debug(f'my_id: {my_id}')
    if hashed_id != my_id:
        app.logger.debug(f'ids do not match <=> key belongs to different shard')
        shard = shard_view_alive[hashed_id][:]
        app.logger.debug(f'shard: {shard}')
        if (len(shard) == 0):
            return '', 418
        server = random.randrange(len(shard))
        app.logger.debug(f'server: {server}')
        app.logger.debug(f'Sending request to http://{shard[server] + route("/" + key)}:')
        app.logger.debug(f'\trequest.headers: {request.headers}')
        app.logger.debug(f'\trequest.get_data(): {request.get_data()}')
        response = requests.put('http://' + shard[server] + route('/' + key), headers=request.headers, data=request.get_data())
        app.logger.debug(f'\tresponse.text: {response.text}')
        app.logger.debug(f'\tresponse.status_code: {response.status_code}')
        app.logger.debug(f'\tresponse.headers.items(): {response.headers.items()}')
        return (response.text, response.status_code, response.headers.items())

    app.logger.debug(f'ids match <=> we can handle this request')
    json_data = request.get_json()
    incoming_addr = request.remote_addr
    app.logger.debug(f'json_data: {json_data}')
    app.logger.debug(f'incoming_addr: {incoming_addr}')
    # Check here if message from fellow servers
    if incoming_addr == my_address_no_port:
        # Ignore messages sent from itself
        app.logger.debug(f'Request was from ourself. Discarding...')
        return format_response('Discarded'), 200
    elif incoming_addr not in shard_view_universe_no_port[my_id]:
        app.logger.debug(f'Request originated outside our shard')
        # Check for causal metadata
        has_metadata = json_data is not None and 'causal-metadata' in json_data and json_data['causal-metadata'] != ''
        if has_metadata:
            while not can_be_delivered_client(json.loads(json_data['causal-metadata'])):
                app.logger.debug(f"Can't deliver request yet, waiting for causal metadata to be synced")
                sleep(5)
        # Forward message here
        app.logger.debug(f'Sending update PUT')
        send_update_put(key, json_data)
        app.logger.debug(f'Attempting to deliver PUT')
        return attempt_deliver_put_message(key, json_data)
    else:
        app.logger.debug(f'Request was from another server inside our shard')
        # Check vector clock here. If out of order, cache
        incoming_vec = json.loads(request.headers.get('VC'))
        app.logger.debug(f'Incoming vector clock')
        if can_be_delivered(incoming_vec, incoming_addr):
            app.logger.debug(f'We can deliver.  Updating vector clock...')
            # deliver message
            vector_clock[incoming_addr] += 1
            update_vector_clock_file()
            app.logger.debug(f'Attempting to deliver PUT message')
            out = attempt_deliver_put_message(key, json_data)
            # deliver all messages in buffer
            app.logger.debug(f'Delivering from buffer')
            deliver_from_buffer()
            return out
        else:
            app.logger.debug(f'Cannot deliver message.  Adding to delivery buffer...')
            delivery_buffer.append([incoming_vec, ['PUT', incoming_addr, key, json_data]])
            app.logger.debug(f'delivery_buffer: {delivery_buffer}')
            app.logger.debug(f'Cached successfully.')
            return format_response('Cached successfully'), 200

@app.route(route('/<key>'), methods=['DELETE'])
def kvs_delete(key):
    update_replicas_view_alive()

    hashed_id = hash(key) % SHARD_COUNT
    my_id = get_my_id()
    if hashed_id != my_id:
        shard = shard_view_alive[hashed_id][:]
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
    if not is_replica(request.remote_addr):
        return jsonify(UNAUTHED_REPLICA_ORIGIN), 401

    request_body = request.get_json()
    target = request_body['socket-address']

    update_replicas_view_alive()

    if target in replicas_view_alive:
        return jsonify(VIEW_PUT_SOCKET_EXISTS), 404
    else:
        replicas_view_alive.add(target)
        return jsonify({
            'message': 'Replica added successfully to the view'}
        ), 200


@app.route(route('-view'), methods=['DELETE'])
def view_delete():
    if not is_replica(request.remote_addr):
        return jsonify(UNAUTHED_REPLICA_ORIGIN), 401

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
