from flask import abort, Flask, request, jsonify, Response
import concurrent.futures
import json
import os
import sys
import requests
import hashlib  # to do hashing

# Project Level Imports
import src.state
import src.heartbeat
# from src.network import
from src.routes import route, route_shard
from src.network import HTTPMethods, multicast
import src.view
# from src.view import VIEW_PUT_SOCKET_EXISTS, update_replicas_view_alive, pull_state, broadcast_add_replica

app = Flask(__name__)


def startup():
    update_vector_clock_file()

    add_replica_fs = src.view.broadcast_add_replica()

    def we_exist_in_view(unicast_response):
        http_response = unicast_response.response
        if http_response is None:
            return False
        added_to_view = http_response.status_code == 200
        preexisted = http_response.status_code == 401 and http_response.json(
        ) == src.view.VIEW_PUT_SOCKET_EXISTS
        return added_to_view or preexisted

    # Get alive replicas from our broadcast.
    unicast_responses = [f.result()
                         for f in concurrent.futures.as_completed(add_replica_fs)]
    unicast_responses_existing = filter(we_exist_in_view, unicast_responses)

    global replicas_view_alive
    replicas_view_alive = set(
        map(lambda ur: ur.address, unicast_responses_existing))
    replicas_view_alive.add(src.state.my_address)
    src.view.update_replicas_view_alive()
    src.view.pull_state()


def determine_port():
    """
    Parses the SOCKET_ADDRESS env variable for the port on which the
    server should run on. If not found, the server runs on port 8080.

    Returns:
        int: The port to run on.
    """
    try:
        port = int(src.state.my_address.split(':')[1])
    except Exception:
        print('Socket Error: Unable to parse port from socket argument.')
        print('Defaulting to port 8080...')
        port = 8080

    return port


def update_vector_clock_file():
    with open('.vector_clock.json', 'w') as f:
        json.dump(src.state.vector_clock, f)


if __name__ == '__main__':
    startup()
    app.run(host='0.0.0.0', port=determine_port())
