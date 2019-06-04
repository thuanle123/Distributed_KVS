# Package and Lib Imports
from collections import defaultdict

# Project Level Imports
import src.heartbeat

store = {}
delivery_buffer = []  # Messages received but not yet delivered.
previously_received_vector_clocks = defaultdict(list)
previously_received_vector_clocks_CAPACITY = 1

replicas_view_universe = src.heartbeat.ADDRESSES
replicas_view_no_port = [x.split(":")[0] for x in replicas_view_universe]
vector_clock = {address: 0 for address in replicas_view_no_port}

my_address = src.heartbeat.MY_ADDRESS
my_address_no_port = my_address.split(":")[0]
replicas_view_alive = {my_address}
replicas_view_alive_filename = src.heartbeat.FILENAME
# When was the last time we read from alive?  Initially never read.
replicas_view_alive_last_read = float('-inf')