# Package and Lib Imports
import json

# Project Level Imports
from src.app import store, delivery_buffer


def format_response(message, does_exist=None, error=None, value=None, replaced=None):
    metadata = json.dumps(vector_clock, sort_keys=True)
    res = {'message': message, 'causal-metadata': metadata, 'version': metadata}

    if does_exist is not None:
        res['doesExist'] = does_exist

    if error is not None:
        res['error'] = error

    if value is not None:
        res['value'] = value

    if replaced is not None:
        res['replaced'] = replaced

    return jsonify(res)

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


def send_update_put(key, message):
    # Sends to all servers (not just alive ones)
    update_replicas_view_alive()
    vector_clock[my_address_no_port] += 1
    update_vector_clock_file()
    headers = {'VC': json.dumps(vector_clock),
               'Content-Type': 'application/json'}
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
    headers = {'VC': json.dumps(vector_clock),
               'Content-Type': 'application/json'}
    multicast(
        replicas_view_alive,
        lambda address: 'http://' + address + route('/' + key),
        http_method=HTTPMethods.DELETE,
        timeout=1,
        headers=headers
    )


@app.route(route(), methods=['GET'])
def store_get():
    resp = {'store': store, 'delivery_buffer': delivery_buffer,
            'vector_clock': vector_clock}
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
        has_metadata = json_data is not None and 'causal-metadata' in json_data and json_data[
            'causal-metadata'] != ''
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
            delivery_buffer.append(
                [incoming_vec, ['PUT', incoming_addr, key, json_data]])
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
        has_metadata = json_data is not None and 'causal-metadata' in json_data and json_data[
            'causal-metadata'] != ''
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
            delivery_buffer.append(
                [incoming_vec, ['DELETE', incoming_addr, key]])
            return format_response('Cached successfully'), 200
