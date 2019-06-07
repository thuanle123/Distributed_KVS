from network import multicast, unicast
from view import init_view
import concurrent.futures
import json
import logging
import os
import random
import time


MY_ADDRESS = os.environ['SOCKET_ADDRESS']
ADDRESSES = init_view()
FILENAME = '.alive.json'
ENDPOINT = '/heartbeat'
INTERVAL = 2.5 # How often to run heartbeat.
TIMEOUT = 2 # Seconds until heartbeat failure.


def address_to_heartbeat_uri(address):
    return 'http://' + address + ENDPOINT


def unicast_heartbeat(address):
    return unicast(
        address,
        address_to_heartbeat_uri,
        timeout=TIMEOUT,
        headers={'VC': json.dumps(get_vector_clock())}
    )


def get_vector_clock():
    with open('.vector_clock.json', 'r') as f:
        vector_clock = json.load(f)
    return vector_clock


def multicast_heartbeat_blocking(addresses):
    logger.info(f'Starting HB multicast: {addresses}')

    fs = multicast(
        addresses,
        address_to_heartbeat_uri,
        timeout=TIMEOUT,
        headers={'VC': json.dumps(get_vector_clock())}
    )
    unicast_responses = [f.result() for f in concurrent.futures.as_completed(fs)]
    alive = [ur.address for ur in unicast_responses if ur.response is not None and ur.response.status_code == 200]
    logger.info(f'Alive: {alive}')
    return alive


def write_alive(addresses, filename):
    deterministic_addresses = sorted(list(addresses))
    random_server = deterministic_addresses[random.randrange(0, len(deterministic_addresses))]
    response = unicast_heartbeat(random_server).response
    random_server_ok = True if response is not None and response.status_code == 200 else False

    try:
        with open(filename, 'r+') as f:
            previous_alive = set(json.load(f))

            if random_server in previous_alive:
                current_alive = previous_alive if random_server_ok else previous_alive.difference({random_server})
            else:
                current_alive = previous_alive.union({random_server}) if random_server_ok else previous_alive

            f.truncate(0)
            f.seek(0)
            json.dump(list(current_alive), f)
    except (FileNotFoundError, IOError):
        current_alive = {MY_ADDRESS, random_server} if random_server_ok else {MY_ADDRESS}
        with open(filename, 'w') as f:
            json.dump(list(current_alive), f)


def run(addresses, filename):
    logger.info(f'HB universe: {addresses}')
    logger.info(f'Writing alive to: {filename}')
    while True:
        start = time.time()

        write_alive(addresses, filename)

        end = time.time()
        elapsed = end - start
        remaining = INTERVAL - elapsed
        if remaining > 0:
            time.sleep(remaining)


if __name__ == '__main__':
    logging.basicConfig(format='[%(levelname)s] %(asctime)s > %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    vector_clock_ready = False
    while not vector_clock_ready:
        try:
            vector_clock = get_vector_clock()
            if vector_clock is not None:
                vector_clock_ready = True
        except:
            pass

    our_server_online = False
    while not our_server_online:
        time.sleep(3)
        unicast_response = unicast_heartbeat(MY_ADDRESS).response
        if unicast_response is not None:
            our_server_online = unicast_response.status_code == 200
    run(ADDRESSES, FILENAME)
