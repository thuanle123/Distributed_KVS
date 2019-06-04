# Package and Lib Imports
import json

# Project Level Imports
from src.app import store, delivery_buffer, vector_clock, replicas_view_alive


UNAUTHED_REPLICA_ORIGIN = {
    'error': 'Request must originate from a replica in the view'}
VIEW_PUT_SOCKET_EXISTS = {
    'error': 'Socket address already exists in the view',
    'message': 'Error in PUT'
}


def init_view():
    """
    Parses the "VIEW" env var and returns the resulting addresses in a list.
    Returns an empty list of the env var is not set or not parsed correctly.

    Returns:
        list: The parsed view.
    """
    try:
        return set(os.environ['VIEW'].split(','))
    except (AttributeError, KeyError) as error:
        print('WARNING: Error parsing "VIEW" env var!')
        return set()


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


def is_replica(ip):
    replica_ips = {address.split(':')[0] for address in replicas_view_universe}
    return ip in replica_ips


def update_replicas_view_alive():
    global replicas_view_alive
    try:
        modified_since_read = os.path.getmtime(
            replicas_view_alive_filename) >= replicas_view_alive_last_read
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


@app.route(route('-view'), methods=['GET'])
def view_get():
    # if not is_replica(request.remote_addr):
    #   return jsonify(UNAUTHED_REPLICA_ORIGIN), 401

    update_replicas_view_alive()
    return jsonify({
        'message': 'View retrieved successfully',
        'view': ",".join(str(x) for x in sorted(replicas_view_alive))
    }), 200


def add_replica_alive(new_address):
    global replicas_view_alive
    try:
        with open(replicas_view_alive_filename, 'r+') as f:
            # Update current alive set.
            replicas_view_alive = set(json.load(f))
            replicas_view_alive.add(my_address)
            replicas_view_alive.add(new_address)

            f.truncate(0)
            f.seek(0)
            json.dump(list(replicas_view_alive), f)  # Add replica.
    except (FileNotFoundError, IOError):
        with open(replicas_view_alive_filename, 'w') as f:
            replicas_view_alive = {my_address, new_address}
            json.dump(list(replicas_view_alive), f)


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
    steady_state = all(
        [incoming_vec == previous_vec for previous_vec in previous_vecs])
    if steady_state:
        if not can_be_delivered_client(incoming_vec):
            pull_state(ip=incoming_addr_with_port)

    # Double ended queue.  Dequeue last VC from the beginning if at capacity.  Enqueue incoming VC to the end.
    if (len(previously_received_vector_clocks[incoming_addr]) > previously_received_vector_clocks_CAPACITY):
        del previously_received_vector_clocks[incoming_addr][0]
    previously_received_vector_clocks[incoming_addr].append(incoming_vec)

    return jsonify({'status': 'OK'}), 200
