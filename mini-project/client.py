#!/usr/bin/env python3
"""client.py - chat client with automatic failover.

* connects to any live node (non-leaders forward to the leader transparently)
* if its node dies, goes silent, or loses the cluster, it reconnects to another node
* resumes from the last delivered sequence number, so nothing is shown twice or skipped
* keeps every message until the cluster delivers it back, and re-sends it if needed
  (the cluster de-duplicates by message id, so retries are safe)

Run:  python client.py --user alice [--nodes 5] [--node 2]
"""
import argparse
import random
import socket
import sys
import threading
import time
import uuid

import config as cfg
from protocol import LineReader, encode


class ChatClient:
    def __init__(self, user, cluster, prefer=None, on_deliver=None, on_event=None):
        self.user, self.cluster = user, cluster
        self.ids = sorted(cluster)
        self.idx = self.ids.index(prefer) if prefer in self.ids else random.randrange(len(self.ids))
        self.on_deliver, self.on_event = on_deliver, on_event
        self.lock = threading.RLock()
        self.sock = None
        self.node_id = None
        self.running = True
        self.last_seq = 0
        self.outbox = {}                  # id -> {"text":..., "sent": timestamp}
        self.delivered = []               # every entry delivered, in order
        self.info = {}
        self.reconnects = 0

    # --------------------------------------------------------------- public API
    def start(self):
        threading.Thread(target=self._run, daemon=True).start()
        return self

    def stop(self):
        self.running = False
        try:
            if self.sock:
                self.sock.close()
        except OSError:
            pass

    def send(self, text):
        mid = uuid.uuid4().hex[:12]
        with self.lock:
            self.outbox[mid] = {"text": text, "sent": 0.0}
        self._transmit(mid)
        return mid

    @property
    def connected(self):
        return self.sock is not None

    # --------------------------------------------------------------- internals
    def _event(self, text):
        if self.on_event:
            self.on_event(text)

    def _transmit(self, mid):
        with self.lock:
            item, sock = self.outbox.get(mid), self.sock
            if not item or not sock:
                return
            try:
                sock.sendall(encode({"type": "CHAT", "id": mid, "user": self.user, "text": item["text"]}))
                item["sent"] = time.time()
            except OSError:
                pass

    def _run(self):
        while self.running:
            nid = self.ids[self.idx % len(self.ids)]
            try:
                s = socket.create_connection(self.cluster[nid], timeout=1.0)
            except OSError:
                self.idx += 1
                time.sleep(0.25)
                continue
            s.settimeout(1.0)
            with self.lock:
                self.sock, self.node_id = s, nid
                self.info = {}
            self.reconnects += 1
            self._event(f"connected to node {nid}")
            try:
                s.sendall(encode({"type": "HELLO", "user": self.user, "resume_from": self.last_seq}))
                for mid in list(self.outbox):
                    self._transmit(mid)
                self._read_loop(s)
            except OSError:
                pass
            with self.lock:
                self.sock = None
            try:
                s.close()
            except OSError:
                pass
            self._event(f"lost node {nid}, trying another")
            self.idx += 1
            time.sleep(0.25)

    def _read_loop(self, s):
        reader = LineReader(s)
        last_info = time.time()
        no_leader_since = None
        while self.running:
            try:
                m = reader.read()
            except socket.timeout:
                m = "timeout"
            except (OSError, ValueError):
                return
            if m is None:
                return
            now = time.time()
            if m == "timeout":
                if now - last_info > 3.0:          # node went silent (crashed or isolated)
                    return
            elif m["type"] in ("INFO", "WELCOME"):
                last_info = now
                self.info = m
                if m.get("leader") is None or not m.get("quorum", True):
                    no_leader_since = no_leader_since or now
                    if now - no_leader_since > 6.0:  # this node cannot reach a leader: move on
                        return
                else:
                    no_leader_since = None
            elif m["type"] == "DELIVER":
                self._deliver(m["entry"])
            for mid, item in list(self.outbox.items()):    # retry anything not delivered back in time
                if item["sent"] and now - item["sent"] > cfg.CLIENT_RESEND_AFTER:
                    self._transmit(mid)

    def _deliver(self, e):
        with self.lock:
            if e["seq"] <= self.last_seq:
                return
            self.last_seq = e["seq"]
            self.outbox.pop(e["id"], None)
            self.delivered.append(e)
        if self.on_deliver:
            self.on_deliver(e)


def main():
    ap = argparse.ArgumentParser(description="Distributed chat client")
    ap.add_argument("--user", required=True)
    ap.add_argument("--node", type=int, help="preferred node id to connect to first")
    cfg.add_cluster_args(ap)
    args = ap.parse_args()
    cluster = cfg.load_cluster(args.config, args.nodes, args.base_port)

    def show(e):
        who = "me" if e["user"] == args.user else e["user"]
        print(f"\r#{e['seq']:<4} [{who}] {e['text']}   (lamport={e['lc']}, via node {e['via']})\n> ", end="", flush=True)

    def note(text):
        print(f"\r* {text}\n> ", end="", flush=True)

    c = ChatClient(args.user, cluster, args.node, show, note).start()
    print("Type a message and press Enter. Commands: /status, /quit")
    try:
        while True:
            line = input("> ").strip()
            if line == "/quit":
                break
            if line == "/status":
                i = c.info
                print(f"  node={c.node_id} leader={i.get('leader')} term={i.get('term')} "
                      f"last_seq={c.last_seq} unacked={len(c.outbox)} reconnects={c.reconnects}")
            elif line:
                c.send(line)
    except (EOFError, KeyboardInterrupt):
        pass
    c.stop()
    sys.exit(0)


if __name__ == "__main__":
    main()
