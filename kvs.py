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

UNAUTHED_REPLICA_ORIGIN = {'error': 'Request must originate from a replica in the view'}
VIEW_PUT_SOCKET_EXISTS = {
    'error': 'Socket address already exists in the view',
    'message': 'Error in PUT'
}

app = Flask(__name__)

store = {}
delivery_buffer = [] # Messages received but not yet delivered.
previously_received_vector_clocks = defaultdict(list)
previously_received_vector_clocks_CAPACITY = 1

replicas_view_universe = heartbeat.ADDRESSES
replicas_view_no_port = [x.split(":")[0] for x in replicas_view_universe]
vector_clock = {address: 0 for address in replicas_view_no_port}

my_address = heartbeat.MY_ADDRESS
my_address_no_port = my_address.split(":")[0]
replicas_view_alive = {my_address}
replicas_view_alive_filename = heartbeat.FILENAME
replicas_view_alive_last_read = float('-inf') # When was the last time we read from alive?  Initially never read.


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
    pull_state()



def pull_state(ip=None):
    global vector_clock
    global delivery_buffer
    global store

    if ip is not None:
       response = requests.get('http://' + ip + route())
       if response.status_code == 200:
           store_with_deliveries = response.json()
           store = store_with_deliveries['store']
           delivery_buffer = store_with_deliveries['delivery_buffer']
           vector_clock = store_with_deliveries['vector_clock']
           return
       return

    other_replicas = replicas_view_alive.difference({my_address})
    if len(other_replicas) <= 0:
        return

    view_fs = multicast(
        other_replicas,
        lambda address: 'http://' + address + route(),
        http_method=HTTPMethods.GET,
        timeout=1
    )
    for f in concurrent.futures.as_completed(view_fs):
        unicast_response = f.result()
        resp = unicast_response.response
        if resp.status_code == 200:
            store_with_deliveries = resp.json()
            # Don't update if we're ahead.
            if can_be_delivered_client(store_with_deliveries['vector_clock']):
                continue
            store = store_with_deliveries['store']
            delivery_buffer = store_with_deliveries['delivery_buffer']
            vector_clock = store_with_deliveries['vector_clock']
            return

def route(r=''):
    return '/key-value-store' + r


def broadcast_add_replica():
    return multicast(
        replicas_view_universe,
        lambda address: 'http://' + address + route('-view'),
        http_method=HTTPMethods.PUT,
        timeout=1,
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
                        multicast,
                        replicas_view_alive,
                        lambda address: 'http://' + address + route('-view'),
                        http_method=HTTPMethods.DELETE,
                        timeout=1,
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
                        timeout=1,
                        data=json.dumps({'socket-address': ar}),
                        headers={'Content-Type': 'application/json'}
                    )

    except (FileNotFoundError, IOError):
        replicas_view_alive = set()

    replicas_view_alive.add(my_address)


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
    # Sends to all servers (not just alive ones)
    update_replicas_view_alive()
    vector_clock[my_address_no_port] += 1
    update_vector_clock_file()
    headers = {'VC': json.dumps(vector_clock), 'Content-Type': 'application/json'}
    multicast(
            replicas_view_alive,
            lambda address: 'http://' + address + route('/' + key),
            http_method=HTTPMethods.PUT,
            timeout=1,
            data=json.dumps(message),
            headers=headers
            )


def send_update_delete(key):
    # Sends to all servers (not just alive ones)
    update_replicas_view_alive()
    vector_clock[my_address_no_port] += 1
    update_vector_clock_file()
    headers = {'VC': json.dumps(vector_clock), 'Content-Type': 'application/json'}
    multicast(
            replicas_view_alive,
            lambda address: 'http://' + address + route('/' + key),
            http_method=HTTPMethods.DELETE,
            timeout=1,
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
    for x in replicas_view_alive:
        cur_addr = x.split(":")[0]
        if vector_clock[cur_addr] < incoming_vec[cur_addr]:
            return False
    return True

def can_be_delivered(incoming_vec, incoming_addr):
    update_replicas_view_alive()
    for x in replicas_view_alive:
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
    if key in store:
        return format_response('Retrieved successfully', does_exist=True, value=store[key]), 200
    return format_response('Error in GET', error='Key does not exist', does_exist=False), 404


@app.route(route('/<key>'), methods=['PUT'])
def kvs_put(key):
    json_data = request.get_json()
    incoming_addr = request.remote_addr
    # Check here if message from fellow servers
    if incoming_addr == my_address_no_port:
        # Ignore messages sent from itself
        return format_response('Discarded'), 200
    elif incoming_addr not in replicas_view_no_port:
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
    # Check here if message from fellow server
    incoming_addr = request.remote_addr
    json_data = request.get_json()
    if incoming_addr == my_address_no_port:
        # Ignore messages sent from itself
        return format_response('Discarded'), 200
    elif incoming_addr not in replicas_view_no_port:
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
    # In the partial script from Reza
    # he has http://localhost send a GET request
    # in section he also said it doesn't need
    # to be from a replica to another replica (like in the SPEC)
    # so i will comment this out for now
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

    if target == my_address:
        pull_state()

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
    if steady_state:
        if not can_be_delivered_client(incoming_vec):
            pull_state(ip=incoming_addr_with_port)

    # Double ended queue.  Dequeue last VC from the beginning if at capacity.  Enqueue incoming VC to the end.
    if (len(previously_received_vector_clocks[incoming_addr]) > previously_received_vector_clocks_CAPACITY):
        del previously_received_vector_clocks[incoming_addr][0]
    previously_received_vector_clocks[incoming_addr].append(incoming_vec)

    return jsonify({'status': 'OK'}), 200


if __name__ == '__main__':
    startup()
    app.run(host='0.0.0.0', port=determine_port())
