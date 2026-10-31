"""protocol.py - wire format (one JSON object per line over TCP) and the Lamport clock.

Peer <-> peer message types
    HEARTBEAT      liveness + leader/term/commit info, sent every HEARTBEAT_INTERVAL
    ELECTION       Bully: "I am starting an election" (sent to higher ids)
    ELECTION_OK    Bully: "I am alive and higher than you, stand down"
    RING_ELECTION  Ring (Chang-Roberts): token carrying the best candidate id so far
    COORDINATOR    "I am the new leader for this term"
    LOG_QUERY / LOG_REPLY   new leader collects peers' logs before serving writes
    FORWARD        follower hands a client's message to the leader
    REPLICATE      leader -> followers: append this log entry
    ACK            follower -> leader: I hold the log up to `upto`
    COMMIT         leader -> followers: entries up to `commit` are safe to deliver
    SYNC_REQ / SYNC_RESP    follower repairs a gap or divergent suffix from the leader

Client <-> node message types
    HELLO {user, resume_from}       client introduces itself and where it stopped reading
    CHAT {id, user, text}           client sends a message (id makes retries idempotent)
    DELIVER {entry}                 node delivers a committed entry (in total order)
    INFO {node, leader, term, ...}  periodic status so clients can detect a dead/isolated node
"""
import json
import threading


class LamportClock:
    """Logical clock: tick() on local events/sends, update(remote) on receives."""

    def __init__(self):
        self._t = 0
        self._lock = threading.Lock()

    def tick(self):
        with self._lock:
            self._t += 1
            return self._t

    def update(self, remote):
        with self._lock:
            self._t = max(self._t, int(remote or 0)) + 1
            return self._t

    @property
    def value(self):
        return self._t


def encode(msg):
    return (json.dumps(msg, separators=(",", ":")) + "\n").encode()


class LineReader:
    """Reads newline-delimited JSON objects from a socket."""

    def __init__(self, sock):
        self.sock = sock
        self.buf = b""

    def read(self):
        """Return the next message, or None if the peer closed. May raise socket.timeout."""
        while b"\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                return None
            self.buf += chunk
        line, self.buf = self.buf.split(b"\n", 1)
        return json.loads(line)
