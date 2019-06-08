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
VECTOR_CLOCK_FILENAME = '.vector_clock.json'
UNIVERSE_FILENAME = '.universe.json'
ALIVE_FILENAME = '.alive.json'
ENDPOINT = '/heartbeat'
INTERVAL = 2.5 # How often to run heartbeat.
TIMEOUT = 2 # Seconds until heartbeat failure.


def address_to_heartbeat_uri(address):
    return 'http://' + address + ENDPOINT


def unicast_heartbeat(address, timeout=TIMEOUT):
    return unicast(
        address,
        address_to_heartbeat_uri,
        timeout=timeout,
        headers={'VC': json.dumps(get_vector_clock())}
    )


def get_replicas_view_universe():
    with open(UNIVERSE_FILENAME, 'r') as f:
        universe = set(json.load(f))
    return universe


def get_vector_clock():
    with open(VECTOR_CLOCK_FILENAME, 'r') as f:
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


def write_alive(address_pool=None, timeout=TIMEOUT):
    if address_pool is None:
        address_pool = sorted(get_replicas_view_universe())

    random_server = address_pool[random.randrange(0, len(address_pool))]
    response = unicast_heartbeat(random_server, timeout=timeout).response
    random_server_ok = True if response is not None and response.status_code == 200 else False

    try:
        with open(ALIVE_FILENAME, 'r+') as f:
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
        with open(ALIVE_FILENAME, 'w') as f:
            json.dump(list(current_alive), f)

    return random_server, random_server_ok


def run():
    logger.info(f'Reading universe from: {UNIVERSE_FILENAME}')
    logger.info(f'Writing alive to: {ALIVE_FILENAME}')

    # Settings for initial runs to populate our alive servers faster.
    # We exhaust the address pool and then fallback to normal functionality.
    in_initial_runs = True
    interval = INTERVAL / 7 # Let intervals be 7x faster.
    exhaust_universe_pool = sorted(get_replicas_view_universe()) # Check every server in our universe exactly once.
    while True:
        start = time.time()

        if in_initial_runs:
            checked_server, checked_server_ok = write_alive(address_pool=exhaust_universe_pool, timeout=0.3)
            if checked_server_ok:
                exhaust_universe_pool.remove(checked_server)
            if len(exhaust_universe_pool) == 0: # We've exhausted our pool.  Fallback to default values.
                in_initial_runs = False
                interval = INTERVAL
        else:
            write_alive()

        end = time.time()
        elapsed = end - start
        remaining = interval - elapsed
        if remaining > 0:
            time.sleep(remaining)

if __name__ == '__main__':
    logging.basicConfig(format='[%(levelname)s] %(asctime)s > %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    universe_ready = False
    while not universe_ready:
        try:
            universe = get_replicas_view_universe()
            if universe is not None:
                universe_ready = True
        except:
            pass

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
    run()
