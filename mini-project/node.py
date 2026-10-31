#!/usr/bin/env python3
"""node.py - one server of the distributed chat cluster.

Responsibilities
  * accept client connections and peer connections on one TCP port
  * heartbeat-based failure detection
  * leader election (Bully or Ring/Chang-Roberts)
  * leader-ordered, quorum-replicated message log (commit when a majority holds an entry)
  * catch-up / sync so restarted or lagging nodes recover the full history
  * fault injection (delay, drop, isolate) and an HTTP status/control API for the dashboard

Run:  python node.py --id 3 [--nodes 5] [--algo bully|ring] [--config cluster.json]
"""
import argparse
import hashlib
import json
import os
import queue
import random
import socket
import sys
import threading
import time
from collections import Counter, OrderedDict, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import config as cfg
from protocol import LamportClock, LineReader, encode

FOLLOWER, ELECTION, LEADER = "FOLLOWER", "ELECTION", "LEADER"
ELECTION_TYPES = ("ELECTION", "ELECTION_OK", "RING_ELECTION", "COORDINATOR")


# --------------------------------------------------------------------------- helpers
class ClientConn:
    """A connected chat client. sent_upto = highest log seq already delivered to it."""

    def __init__(self, sock, addr):
        self.sock, self.addr = sock, addr
        self.user = "?"
        self.sent_upto = 0
        self.alive = True
        self._wlock = threading.Lock()

    def send(self, msg):
        try:
            with self._wlock:
                self.sock.sendall(encode(msg))
            return True
        except OSError:
            self.alive = False
            return False


class PeerLink(threading.Thread):
    """Outbound FIFO channel to one peer. Keeps message order, applies injected faults."""

    def __init__(self, node, pid):
        super().__init__(daemon=True)
        self.node, self.pid = node, pid
        self.addr = node.cluster[pid]
        self.q = queue.Queue(maxsize=500)
        self.sock = None

    def send(self, msg):
        try:
            self.q.put_nowait(msg)
        except queue.Full:                       # drop the oldest rather than block
            try:
                self.q.get_nowait()
                self.q.put_nowait(msg)
            except (queue.Empty, queue.Full):
                pass

    def _connect(self):
        try:
            s = socket.create_connection(self.addr, timeout=0.5)
            s.settimeout(2.0)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.sock = s
            return True
        except OSError:
            self.sock = None
            return False

    def _transmit(self, msg):
        data = encode(msg)
        for _ in range(2):
            if self.sock is None and not self._connect():
                return False
            try:
                self.sock.sendall(data)
                return True
            except OSError:
                try:
                    self.sock.close()
                except OSError:
                    pass
                self.sock = None
        return False

    def run(self):
        while self.node.running:
            try:
                msg = self.q.get(timeout=0.5)
            except queue.Empty:
                continue
            f = self.node.faults
            if self.pid in f["blocked"]:
                continue                                       # simulated partition
            if f["drop"] > 0 and random.random() < f["drop"]:
                continue                                       # simulated packet loss
            if f["delay_ms"] > 0:                              # simulated latency + jitter
                time.sleep(f["delay_ms"] / 1000.0 * (0.5 + random.random()))
            if not self._transmit(msg):
                while True:                                    # peer is down: do not replay stale traffic later
                    try:
                        self.q.get_nowait()
                    except queue.Empty:
                        break


# --------------------------------------------------------------------------- node
class Node:
    def __init__(self, node_id, cluster, algo="bully"):
        self.id, self.cluster, self.algo = node_id, cluster, algo
        self.peers = sorted(p for p in cluster if p != node_id)
        self.majority = len(cluster) // 2 + 1
        self.lock = threading.RLock()
        self.flush_lock = threading.Lock()
        self.clock = LamportClock()
        self.running = True
        self.boot = time.time()

        # cluster / election state
        self.state = FOLLOWER
        self.leader_id = None
        self.term = 0                         # highest term seen in any message
        self.lead_term = 0                    # term under which THIS node leads (0 if it never led)
        self.max_lead_term = 0                # highest leader term seen: older leaders are stale
        self.ready = False                    # leader only: caught up and allowed to serve writes
        self.last_seen = {p: 0.0 for p in self.peers}
        self.election_id = 0
        self.election_started = 0.0
        self.got_ok = False
        self.participant = False
        self.last_reassert = 0.0

        # replicated log
        self.log = []                         # entries, log[i]["seq"] == i + 1
        self.ids = {}                         # message id -> seq
        self.commit_index = 0
        self.acks = {}                        # leader: seq -> set(node ids holding it)
        self.catchup = None
        self.last_sync_req = 0.0
        self.pending = deque(maxlen=1000)     # messages waiting for a usable leader
        self.fwd_buffer = OrderedDict()       # messages forwarded but not yet seen in the log

        # clients, faults, observability
        self.clients = {}
        self._client_seq = 0
        self.faults = {"delay_ms": 0, "drop": 0.0, "blocked": set()}
        self.msg_counts = Counter()
        self.events = deque(maxlen=300)
        self.elections = deque(maxlen=20)

        self.links = {p: PeerLink(self, p) for p in self.peers}
        self.handlers = {
            "HEARTBEAT": self.on_heartbeat, "ELECTION": self.on_election,
            "ELECTION_OK": self.on_election_ok, "RING_ELECTION": self.on_ring,
            "COORDINATOR": self.on_coordinator, "LOG_QUERY": self.on_log_query,
            "LOG_REPLY": self.on_log_reply, "FORWARD": self.on_forward,
            "REPLICATE": self.on_replicate, "ACK": self.on_ack, "COMMIT": self.on_commit,
            "SYNC_REQ": self.on_sync_req, "SYNC_RESP": self.on_sync_resp,
        }

    # ------------------------------------------------------------------ utilities
    def log_event(self, text, kind="info"):
        now = time.time()
        self.events.append({"t": now, "node": self.id, "kind": kind, "text": text})
        stamp = time.strftime("%H:%M:%S", time.localtime(now)) + ".%03d" % int((now % 1) * 1000)
        print(f"[N{self.id} {stamp}] {text}", flush=True)

    def alive_peers(self):
        now = time.time()
        return [p for p in self.peers if now - self.last_seen[p] < cfg.FAILURE_TIMEOUT]

    def has_quorum(self):
        return len(self.alive_peers()) + 1 >= self.majority

    def send(self, pid, msg):
        msg = dict(msg)
        msg["from"] = self.id
        msg["lc"] = self.clock.tick()
        if msg["type"] in ELECTION_TYPES:
            self.msg_counts[msg["type"]] += 1
        self.msg_counts["total"] += 1
        link = self.links.get(pid)
        if link:
            link.send(msg)

    def broadcast(self, msg):
        for p in self.peers:
            self.send(p, msg)

    # ------------------------------------------------------------------ network plumbing
    def start(self):
        for link in self.links.values():
            link.start()
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        host, port = self.cluster[self.id]
        srv.bind(("0.0.0.0", port))
        srv.listen(64)
        self.srv = srv
        threading.Thread(target=self._accept_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        threading.Thread(target=self._monitor_loop, daemon=True).start()
        self._start_http(port + cfg.HTTP_OFFSET)
        self.log_event(f"node started on port {port} (algo={self.algo}, cluster size={len(self.cluster)})")

    def _accept_loop(self):
        while self.running:
            try:
                sock, addr = self.srv.accept()
            except OSError:
                break
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(30)
            threading.Thread(target=self._handle_conn, args=(sock, addr), daemon=True).start()

    def _handle_conn(self, sock, addr):
        reader, client = LineReader(sock), None
        try:
            while self.running:
                try:
                    m = reader.read()
                except socket.timeout:
                    continue
                if m is None:
                    break
                t = m.get("type")
                if t == "HELLO":
                    client = self._register_client(sock, addr, m)
                elif t == "CHAT":
                    self.submit({"id": m["id"], "user": m.get("user", "?"), "text": m["text"]})
                else:
                    self._dispatch_peer(m)
        except (OSError, ValueError, KeyError):
            pass
        finally:
            if client:
                client.alive = False
                with self.lock:
                    self.clients.pop(id(client), None)
            try:
                sock.close()
            except OSError:
                pass

    def _dispatch_peer(self, m):
        frm = m.get("from")
        if frm not in self.last_seen or frm in self.faults["blocked"]:
            return
        self.clock.update(m.get("lc"))
        self.last_seen[frm] = time.time()
        handler = self.handlers.get(m.get("type"))
        if handler:
            handler(m)

    # ------------------------------------------------------------------ clients
    def _register_client(self, sock, addr, m):
        c = ClientConn(sock, addr)
        c.user = m.get("user", "?")
        c.sent_upto = int(m.get("resume_from", 0))
        with self.lock:
            self.clients[id(c)] = c
        c.send(self._info_msg("WELCOME"))
        self.log_event(f"client '{c.user}' connected (resume from seq {c.sent_upto})")
        self._flush_clients()
        return c

    def _info_msg(self, kind="INFO"):
        with self.lock:
            return {"type": kind, "node": self.id, "leader": self.leader_id, "term": self.max_lead_term,
                    "state": self.state, "quorum": self.has_quorum()}

    def _notify_clients_info(self):
        info = self._info_msg()
        for c in list(self.clients.values()):
            c.send(info)

    def _flush_clients(self):
        """Deliver every committed entry a client has not yet received, in order."""
        with self.flush_lock:
            with self.lock:
                ci = self.commit_index
                batch = [(c, self.log[c.sent_upto:ci]) for c in self.clients.values() if c.alive]
            for c, entries in batch:
                for e in entries:
                    if not c.send({"type": "DELIVER", "entry": e}):
                        break
                    c.sent_upto = e["seq"]

    # ------------------------------------------------------------------ background loops
    def _heartbeat_loop(self):
        while self.running:
            with self.lock:
                ci = self.commit_index
                hb = {"type": "HEARTBEAT", "state": self.state, "leader": self.leader_id,
                      "term": self.lead_term if self.state == LEADER else self.term,
                      "last_seq": len(self.log), "commit": ci,
                      "commit_id": self.log[ci - 1]["id"] if ci else None}
            self.broadcast(hb)
            self._notify_clients_info()
            time.sleep(cfg.HEARTBEAT_INTERVAL)

    def _monitor_loop(self):
        last_flush = 0.0
        while self.running:
            time.sleep(0.2)
            now = time.time()
            with self.lock:
                state, leader = self.state, self.leader_id
            if state == FOLLOWER:
                if leader is not None and now - self.last_seen[leader] > cfg.FAILURE_TIMEOUT:
                    self.log_event(f"leader N{leader} stopped sending heartbeats -> presumed DEAD", "failure")
                    with self.lock:
                        self.leader_id = None
                    self.start_election(f"leader N{leader} failed")
                elif leader is None and now - self.boot > cfg.STARTUP_GRACE:
                    self.start_election("no leader known")
            elif state == ELECTION:
                if now - self.election_started > cfg.ELECTION_WAIT_OK + cfg.ELECTION_WAIT_COORD + 2:
                    self.start_election("election stalled", force=True)
            if now - last_flush > 1.0:
                last_flush = now
                if self.pending or self.fwd_buffer:
                    self._flush_pending()

    # ------------------------------------------------------------------ election: common
    def start_election(self, reason, force=False, initiate=True):
        with self.lock:
            if self.state == ELECTION and not force:
                return
            self.state = ELECTION
            self.leader_id = None
            self.ready = False
            self.election_id += 1
            eid = self.election_id
            self.election_started = time.time()
            self.got_ok = False
            self.participant = False
            self.log_event(f"ELECTION started ({self.algo}): {reason}", "election")
        if initiate:
            target = self._bully_run if self.algo == "bully" else self._ring_run
            threading.Thread(target=target, args=(eid,), daemon=True).start()
        else:
            threading.Thread(target=self._watch_election, args=(eid,), daemon=True).start()

    def _election_current(self, eid):
        return self.election_id == eid and self.state == ELECTION

    def _watch_election(self, eid):
        deadline = time.time() + cfg.ELECTION_WAIT_COORD
        while time.time() < deadline:
            if not self._election_current(eid):
                return
            time.sleep(0.05)
        if self._election_current(eid):
            self.start_election("coordinator timeout", force=True)

    def _finish_election(self, winner):
        if self.election_started:
            dur = time.time() - self.election_started
            self.elections.append({"t": time.time(), "algo": self.algo, "winner": winner,
                                   "duration": round(dur, 3), "term": self.term})
            self.election_started = 0.0

    def _become_leader(self):
        with self.lock:
            self.term += 1
            term = self.term
            self.lead_term = term
            self.max_lead_term = max(self.max_lead_term, term)
            self.state, self.leader_id, self.ready = LEADER, self.id, False
            self.election_id += 1
            self.participant = False
            self._finish_election(self.id)
            self.acks = {}
            self.last_reassert = time.time()
            self.log_event(f"became LEADER (term {term}) - collecting peer logs before serving", "leader")
        self.broadcast({"type": "COORDINATOR", "term": term})
        self._notify_clients_info()
        threading.Thread(target=self._leader_catchup, args=(term,), daemon=True).start()

    def _set_leader(self, frm, term):
        with self.lock:
            was_leader, was_state = self.leader_id, self.state
            self.term = max(self.term, term)
            self.max_lead_term = max(self.max_lead_term, term)
            self.state, self.leader_id, self.ready = FOLLOWER, frm, False
            self.last_seen[frm] = time.time()
            self.election_id += 1                      # cancels any running election threads
            self.participant = False
            changed = was_leader != frm or was_state != FOLLOWER
            if changed:
                self._finish_election(frm)
                self.log_event(f"accepted N{frm} as LEADER (term {term})", "leader")
        if changed:
            self._notify_clients_info()
            threading.Thread(target=self._flush_pending, daemon=True).start()

    def on_coordinator(self, m):
        frm, term = m["from"], m["term"]
        with self.lock:
            self.term = max(self.term, term)
            am_leader, stale = self.state == LEADER, term < self.max_lead_term
        if stale:                                      # announcement from a superseded leader
            return
        if frm < self.id:                              # a lower id must not rule while I live
            if am_leader:
                self._reassert()
            else:
                self.start_election(f"N{frm} announced itself but I have a higher id")
            return
        self._set_leader(frm, term)

    def _reassert(self):
        """Become leader again under a fresh, higher term (re-runs the log catch-up)."""
        if time.time() - self.last_reassert > 1.0:
            self.last_reassert = time.time()
            threading.Thread(target=self._become_leader, daemon=True).start()

    # ------------------------------------------------------------------ election: Bully
    def _bully_run(self, eid):
        higher = [p for p in self.peers if p > self.id]
        alive_higher = [p for p in higher if p in self.alive_peers()]
        for p in higher:
            self.send(p, {"type": "ELECTION"})
        if alive_higher:
            deadline = time.time() + cfg.ELECTION_WAIT_OK
            while time.time() < deadline and self._election_current(eid) and not self.got_ok:
                time.sleep(0.03)
        if not self._election_current(eid):
            return
        if not self.got_ok:
            self._become_leader()
            return
        deadline = time.time() + cfg.ELECTION_WAIT_COORD       # a higher node took over; wait for it
        while time.time() < deadline:
            if not self._election_current(eid):
                return
            time.sleep(0.05)
        if self._election_current(eid):
            self.start_election("no COORDINATOR after OK", force=True)

    def on_election(self, m):
        frm = m["from"]
        self.send(frm, {"type": "ELECTION_OK"})
        with self.lock:
            lead = self.state == LEADER
            term = self.lead_term
        if lead:
            self.send(frm, {"type": "COORDINATOR", "term": term})
        elif frm < self.id:
            self.start_election(f"challenged by N{frm}")

    def on_election_ok(self, m):
        self.got_ok = True

    # ------------------------------------------------------------------ election: Ring (Chang-Roberts)
    def _ring_next(self):
        ids = sorted(self.cluster)
        alive = set(self.alive_peers()) | {self.id}
        i = ids.index(self.id)
        for k in range(1, len(ids)):
            cand = ids[(i + k) % len(ids)]
            if cand in alive:
                return cand
        return None

    def _ring_run(self, eid):
        with self.lock:
            self.participant = True
        nxt = self._ring_next()
        if nxt is None:
            self._become_leader()
            return
        self.send(nxt, {"type": "RING_ELECTION", "cand": self.id})
        deadline = time.time() + cfg.ELECTION_WAIT_COORD
        while time.time() < deadline:
            if not self._election_current(eid):
                return
            time.sleep(0.05)
        if self._election_current(eid):
            self.start_election("ring token lost", force=True)

    def on_ring(self, m):
        frm, cand = m["from"], m["cand"]
        with self.lock:
            lead, term = self.state == LEADER, self.lead_term
        if lead and cand < self.id:
            # a candidate that cannot beat the sitting leader: just re-announce leadership to everyone
            if time.time() - self.last_reassert > 1.0:
                self.last_reassert = time.time()
                self.broadcast({"type": "COORDINATOR", "term": term})
            return
        if self.state != ELECTION:
            self.start_election(f"ring token from N{frm}", initiate=False)
        with self.lock:
            if cand > self.id:
                self.participant, fwd = True, cand
            elif cand < self.id:
                if self.participant:
                    return                                     # already took part with a better id
                self.participant, fwd = True, self.id
            else:
                fwd = None                                     # my own id came back: I win
        if fwd is None:
            self._become_leader()
            return
        nxt = self._ring_next()
        if nxt is None:
            self._become_leader()
        else:
            self.send(nxt, {"type": "RING_ELECTION", "cand": fwd})

    # ------------------------------------------------------------------ heartbeats
    def on_heartbeat(self, m):
        frm, mterm = m["from"], m.get("term", 0)
        is_leader_hb = m.get("state") == LEADER
        with self.lock:
            self.term = max(self.term, mterm)
            st, cur, lt, max_lt = self.state, self.leader_id, self.lead_term, self.max_lead_term
        if st == LEADER:
            if mterm > lt:                             # someone has seen a newer term than mine: I am stale
                if is_leader_hb and frm > self.id:
                    self._set_leader(frm, mterm)
                else:
                    self._reassert()                   # re-win under a higher term, re-syncing my log first
            elif is_leader_hb and mterm == lt and frm > self.id:
                self._set_leader(frm, mterm)           # simultaneous leaders: the higher id wins
            return
        if not is_leader_hb or mterm < max_lt:
            return                                     # follower heartbeat, or a superseded leader
        if frm > self.id or cur == frm:
            if cur != frm or st != FOLLOWER:
                self._set_leader(frm, mterm)
            self._advance_commit(m.get("commit", 0), m.get("commit_id"), frm)
            if m.get("last_seq", 0) > len(self.log):
                self._request_sync(frm)
        else:
            self.start_election(f"leader N{frm} has a lower id than mine")

    # ------------------------------------------------------------------ messages: submit / forward
    def submit(self, entry):
        with self.lock:
            if entry["id"] in self.ids:
                return
            st, ready, leader = self.state, self.ready, self.leader_id
            if st == LEADER and ready and self.has_quorum():
                mode = "append"
            elif st == FOLLOWER and leader is not None:
                mode = "forward"
                self.fwd_buffer[entry["id"]] = entry
                while len(self.fwd_buffer) > 1000:
                    self.fwd_buffer.popitem(last=False)
            else:
                self.pending.append(entry)
                return
        if mode == "append":
            self._leader_append(entry)
        else:
            self.send(leader, {"type": "FORWARD", "entry": entry})

    def on_forward(self, m):
        entry = dict(m["entry"])
        entry["hops"] = entry.get("hops", 0) + 1
        if entry["hops"] <= 3:
            self.submit(entry)

    def _flush_pending(self):
        with self.lock:
            items = list(self.pending)
            self.pending.clear()
            for mid in [i for i in self.fwd_buffer if i in self.ids]:
                self.fwd_buffer.pop(mid)
            items += [e for e in self.fwd_buffer.values() if e["id"] not in self.ids]
        for e in items:
            self.submit(dict(e))

    # ------------------------------------------------------------------ replication (leader side)
    def _leader_append(self, entry):
        with self.lock:
            if entry["id"] in self.ids or self.state != LEADER:
                return
            seq = len(self.log) + 1
            e = {"seq": seq, "id": entry["id"], "user": entry["user"], "text": entry["text"],
                 "lc": self.clock.tick(), "term": self.lead_term, "ts": round(time.time(), 3), "via": self.id}
            self.log.append(e)
            self.ids[e["id"]] = seq
            self.acks[seq] = {self.id}
        self._replicate(seq)
        self._try_commit()

    def _replicate(self, seq):
        with self.lock:
            e = self.log[seq - 1]
            prev_id = self.log[seq - 2]["id"] if seq > 1 else None
            term = self.lead_term
        self.broadcast({"type": "REPLICATE", "term": term, "entry": e, "prev_id": prev_id})

    def on_ack(self, m):
        with self.lock:
            if self.state != LEADER or m.get("term") != self.lead_term:
                return
            for s in range(self.commit_index + 1, min(m["upto"], len(self.log)) + 1):
                self.acks.setdefault(s, {self.id}).add(m["from"])
        self._try_commit()

    def _try_commit(self):
        advanced = False
        with self.lock:
            if self.state != LEADER:
                return
            while self.commit_index < len(self.log):
                s = self.commit_index + 1
                if len(self.acks.get(s, {self.id})) >= self.majority:
                    self.commit_index = s
                    self.acks.pop(s, None)
                    advanced = True
                else:
                    break
            ci = self.commit_index
            cid = self.log[ci - 1]["id"] if ci else None
        if advanced:
            self.broadcast({"type": "COMMIT", "commit": ci, "commit_id": cid})
            self._flush_clients()

    def _leader_catchup(self, term):
        """A fresh leader first adopts the most complete log among live peers, then serves writes."""
        while self.running:                                    # no quorum => cannot safely lead
            with self.lock:
                if self.state != LEADER or self.lead_term != term:
                    return
            if self.has_quorum():
                break
            time.sleep(0.3)
        alive = self.alive_peers()
        with self.lock:
            self.catchup = {"expect": set(alive), "replies": {}}
            from_seq = self.commit_index + 1
        for p in alive:
            self.send(p, {"type": "LOG_QUERY", "from_seq": from_seq})
        deadline = time.time() + cfg.CATCHUP_WAIT
        while time.time() < deadline:
            with self.lock:
                if self.state != LEADER or self.lead_term != term:
                    return
                if set(self.catchup["replies"]) >= self.catchup["expect"]:
                    break
            time.sleep(0.02)
        with self.lock:
            if self.state != LEADER or self.lead_term != term:
                return
            base = self.commit_index
            best, commit = self.log[base:], base
            for entries, pc in self.catchup["replies"].values():
                commit = max(commit, pc)
                if len(entries) > len(best):
                    best = entries
            self.catchup = None
            done = {e["id"] for e in self.log[:base]}
            suffix = []
            for e in best:
                if e["id"] in done:
                    continue
                e = dict(e)
                e["seq"] = base + len(suffix) + 1
                suffix.append(e)
            del self.log[base:]
            self.log.extend(suffix)
            self.ids = {e["id"]: e["seq"] for e in self.log}
            self.commit_index = min(max(base, commit), len(self.log))
            self.acks = {s: {self.id} for s in range(self.commit_index + 1, len(self.log) + 1)}
            self.ready = True
            n_log, n_commit = len(self.log), self.commit_index
            self.log_event(f"leader ready: log={n_log} commit={n_commit} (history synced from peers)", "leader")
        for s in range(n_commit + 1, n_log + 1):
            self._replicate(s)
        self._try_commit()
        self._flush_clients()
        self._flush_pending()

    def on_log_query(self, m):
        with self.lock:
            entries = self.log[m["from_seq"] - 1:]
            commit = self.commit_index
        self.send(m["from"], {"type": "LOG_REPLY", "entries": entries, "commit": commit})

    def on_log_reply(self, m):
        with self.lock:
            if self.catchup is not None:
                self.catchup["replies"][m["from"]] = (m["entries"], m["commit"])

    # ------------------------------------------------------------------ replication (follower side)
    def on_replicate(self, m):
        frm, term = m["from"], m["term"]
        with self.lock:
            if term < self.max_lead_term:
                return                                 # stale leader
            need_adopt = self.leader_id != frm or self.state != FOLLOWER
        if need_adopt:
            if frm < self.id:
                return
            self._set_leader(frm, term)
        e, s, prev_id = m["entry"], m["entry"]["seq"], m["prev_id"]
        need_sync = False
        with self.lock:
            n = len(self.log)
            ok = s == 1 or (n >= s - 1 and self.log[s - 2]["id"] == prev_id)
            if ok:
                if s <= n:
                    if self.log[s - 1]["id"] != e["id"]:
                        if s <= self.commit_index:
                            ok, need_sync = False, True
                        else:                                      # divergent uncommitted suffix: replace
                            del self.log[s - 1:]
                            self.log.append(e)
                            self.ids = {x["id"]: x["seq"] for x in self.log}
                else:
                    self.log.append(e)
                    self.ids[e["id"]] = s
            else:
                need_sync = True
        if need_sync:
            self._request_sync(frm)
            return
        self.send(frm, {"type": "ACK", "term": term, "upto": s})

    def on_commit(self, m):
        self._advance_commit(m["commit"], m.get("commit_id"), m["from"])

    def _advance_commit(self, commit, commit_id, frm):
        flush = need_sync = False
        with self.lock:
            if self.state != FOLLOWER or self.leader_id != frm or commit <= self.commit_index:
                return
            if commit <= len(self.log) and self.log[commit - 1]["id"] == commit_id:
                self.commit_index = commit
                flush = True
            else:
                need_sync = True
        if flush:
            self._flush_clients()
        if need_sync:
            self._request_sync(frm)

    def _request_sync(self, leader):
        now = time.time()
        if now - self.last_sync_req < 0.3:
            return
        self.last_sync_req = now
        with self.lock:
            from_seq = self.commit_index + 1
        self.send(leader, {"type": "SYNC_REQ", "from_seq": from_seq})

    def on_sync_req(self, m):
        with self.lock:
            if self.state != LEADER or not self.ready:
                return
            entries = self.log[m["from_seq"] - 1:]
            payload = {"type": "SYNC_RESP", "term": self.lead_term, "from_seq": m["from_seq"],
                       "entries": entries, "commit": self.commit_index}
        self.send(m["from"], payload)

    def on_sync_resp(self, m):
        frm = m["from"]
        with self.lock:
            if self.leader_id != frm:
                return
            start = max(m["from_seq"], self.commit_index + 1)
            if start - 1 > len(self.log):
                return
            fresh = [e for e in m["entries"] if e["seq"] >= start]
            del self.log[start - 1:]
            self.log.extend(fresh)
            self.ids = {e["id"]: e["seq"] for e in self.log}
            self.commit_index = max(self.commit_index, min(m["commit"], len(self.log)))
            upto, term = len(self.log), m["term"]
            self.log_event(f"synced log from leader N{frm}: now {upto} entries, commit {self.commit_index}", "sync")
        self._flush_clients()
        self.send(frm, {"type": "ACK", "term": term, "upto": upto})

    # ------------------------------------------------------------------ status + control (HTTP)
    def digest(self):
        with self.lock:
            ids = ",".join(e["id"] for e in self.log[:self.commit_index])
        return hashlib.md5(ids.encode()).hexdigest()[:8]

    def status(self):
        with self.lock:
            return {
                "id": self.id, "algo": self.algo, "state": self.state, "leader": self.leader_id,
                "term": self.max_lead_term, "ready": self.ready, "quorum": self.has_quorum(),
                "alive_peers": self.alive_peers(), "log_len": len(self.log), "commit": self.commit_index,
                "digest": self.digest(), "clients": [c.user for c in self.clients.values() if c.alive],
                "lamport": self.clock.value, "faults": {"delay_ms": self.faults["delay_ms"],
                                                         "drop": self.faults["drop"],
                                                         "isolated": sorted(self.faults["blocked"])},
                "counts": dict(self.msg_counts), "elections": list(self.elections),
                "events": list(self.events)[-60:], "recent": self.log[max(0, self.commit_index - 15):self.commit_index],
                "uptime": round(time.time() - self.boot, 1), "pending": len(self.pending),
            }

    def control(self, body):
        cmd = body.get("cmd")
        if cmd == "fault":
            self.faults["delay_ms"] = max(0, int(body.get("delay_ms", 0)))
            self.faults["drop"] = min(1.0, max(0.0, float(body.get("drop", 0))))
            self.log_event(f"fault injection: delay={self.faults['delay_ms']}ms drop={self.faults['drop']:.0%}", "fault")
        elif cmd == "isolate":
            self.faults["blocked"] = set(self.peers)
            self.log_event("NETWORK PARTITION: this node is now isolated from all peers", "fault")
        elif cmd == "heal":
            self.faults.update(blocked=set(), delay_ms=0, drop=0.0)
            self.log_event("network healed: faults cleared", "fault")
        elif cmd == "reset_metrics":
            self.msg_counts.clear()
            self.elections.clear()
        elif cmd == "kill":
            self.log_event("CRASH injected (process exits)", "failure")
            threading.Timer(0.1, lambda: os._exit(1)).start()
        else:
            return {"ok": False, "error": "unknown cmd"}
        return {"ok": True}

    def _start_http(self, port):
        node = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, obj, code=200):
                data = json.dumps(obj).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_OPTIONS(self):
                self.send_response(204)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.end_headers()

            def do_GET(self):
                if self.path.startswith("/status"):
                    self._send(node.status())
                else:
                    self._send({"error": "not found"}, 404)

            def do_POST(self):
                try:
                    n = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(n) or b"{}")
                    self._send(node.control(body))
                except Exception as exc:                      # noqa: BLE001
                    self._send({"ok": False, "error": str(exc)}, 400)

        srv = ThreadingHTTPServer(("0.0.0.0", port), H)
        srv.daemon_threads = True
        threading.Thread(target=srv.serve_forever, daemon=True).start()


def main():
    ap = argparse.ArgumentParser(description="Distributed chat server node")
    ap.add_argument("--id", type=int, required=True)
    ap.add_argument("--algo", choices=["bully", "ring"], default="bully")
    cfg.add_cluster_args(ap)
    args = ap.parse_args()
    cluster = cfg.load_cluster(args.config, args.nodes, args.base_port)
    if args.id not in cluster:
        sys.exit(f"node id {args.id} is not in the cluster {sorted(cluster)}")
    node = Node(args.id, cluster, args.algo)
    node.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.running = False


if __name__ == "__main__":
    main()
