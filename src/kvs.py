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