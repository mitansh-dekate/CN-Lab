"""config.py - cluster layout and timing constants shared by every component.

Default layout: N nodes on localhost, node i listens on BASE_PORT + i for peers and
clients, and on BASE_PORT + i + 100 for its HTTP status/control API.

To run on several laptops, write a cluster.json like

    {"nodes": {"1": {"host": "192.168.1.10", "port": 9001},
               "2": {"host": "192.168.1.11", "port": 9002}}}

and pass --config cluster.json to node.py, client.py and dashboard.py.
"""
import json

DEFAULT_NODES = 5
BASE_PORT = 9000
HOST = "127.0.0.1"
HTTP_OFFSET = 100

# Timing (seconds). Small enough for a lively demo, large enough to avoid false alarms.
HEARTBEAT_INTERVAL = 0.5     # how often every node pings its peers
FAILURE_TIMEOUT = 1.5        # silence longer than this => peer presumed dead
STARTUP_GRACE = 1.5          # wait for heartbeats before the very first election
ELECTION_WAIT_OK = 1.0       # Bully: wait this long for an OK from a higher node
ELECTION_WAIT_COORD = 3.0    # wait this long for a COORDINATOR announcement
CATCHUP_WAIT = 1.0           # new leader waits this long for peers' logs
CLIENT_RESEND_AFTER = 4.0    # client re-sends a message that was not delivered back


def load_cluster(path=None, n=DEFAULT_NODES, base_port=BASE_PORT, host=HOST):
    """Return {node_id: (host, port)}."""
    if path:
        with open(path) as f:
            raw = json.load(f)
        return {int(k): (v["host"], int(v["port"])) for k, v in raw["nodes"].items()}
    return {i: (host, base_port + i) for i in range(1, n + 1)}


def http_port(port):
    return port + HTTP_OFFSET


def add_cluster_args(parser):
    parser.add_argument("--config", help="cluster.json for multi-machine runs")
    parser.add_argument("--nodes", type=int, default=DEFAULT_NODES, help="number of nodes (localhost layout)")
    parser.add_argument("--base-port", type=int, default=BASE_PORT, help="node i uses base-port + i")
