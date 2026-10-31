#!/usr/bin/env python3
"""dashboard.py - live cluster console for the demo.

    python dashboard.py --spawn            # start 5 nodes AND the dashboard, open http://localhost:8080
    python dashboard.py --spawn --algo ring
    python dashboard.py --config cluster.json   # monitor nodes that already run elsewhere

Shows node states, the current leader and term, election/failure events, per-node logs,
message counters and a chat panel. Buttons inject failures (crash, restart, partition,
latency, packet loss) so the whole story can be told live.
"""
import argparse
import json
import os
import threading
import time
import urllib.request
import webbrowser
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import config as cfg
from client import ChatClient
from cluster import Cluster

HERE = os.path.dirname(os.path.abspath(__file__))


class Dashboard:
    def __init__(self, cluster_cfg, manager=None, algo="bully"):
        self.cfg, self.mgr, self.algo = cluster_cfg, manager, algo
        self.snapshot = {i: None for i in cluster_cfg}
        self.pool = ThreadPoolExecutor(max_workers=len(cluster_cfg))
        self.chat_events = deque(maxlen=30)
        self.web = ChatClient("web-ui", cluster_cfg, on_event=self._chat_event).start()
        threading.Thread(target=self._poll_loop, daemon=True).start()

    def _chat_event(self, text):
        self.chat_events.append({"t": time.time(), "text": text})

    def _fetch(self, nid):
        host, port = self.cfg[nid]
        try:
            with urllib.request.urlopen(f"http://{host}:{cfg.http_port(port)}/status", timeout=0.45) as r:
                return nid, json.load(r)
        except Exception:                                    # noqa: BLE001
            return nid, None

    def _poll_loop(self):
        while True:
            self.snapshot = dict(self.pool.map(self._fetch, list(self.cfg)))
            time.sleep(0.35)

    def api_cluster(self):
        nodes = {}
        for nid, st in self.snapshot.items():
            nodes[nid] = {"status": st, "managed": bool(self.mgr), "running": self.mgr.is_running(nid) if self.mgr else st is not None}
        c = self.web
        return {"algo": self.algo, "now": time.time(), "nodes": nodes, "managed": bool(self.mgr),
                "chat": {"connected_to": c.node_id, "last_seq": c.last_seq, "unacked": len(c.outbox),
                         "messages": c.delivered[-80:], "events": list(self.chat_events)[-8:]}}

    def control(self, body):
        nid, cmd = int(body["node"]), body["cmd"]
        if self.mgr and cmd == "kill":
            self.mgr.kill(nid)
            return {"ok": True}
        if self.mgr and cmd == "start":
            self.mgr.start(nid)
            return {"ok": True}
        host, port = self.cfg[nid]
        payload = {k: v for k, v in body.items() if k != "node"}
        req = urllib.request.Request(f"http://{host}:{cfg.http_port(port)}/ctl", json.dumps(payload).encode(),
                                     {"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=1.0) as r:
                return json.load(r)
        except Exception as exc:                             # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    def send(self, user, text):
        self.web.user = (user or "web-ui")[:24]
        self.web.send(text[:500])
        return {"ok": True}


def make_handler(dash):
    page = open(os.path.join(HERE, "dashboard.html"), "rb").read()

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _json(self, obj, code=200):
            data = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path == "/api/cluster":
                self._json(dash.api_cluster())
            elif self.path in ("/", "/index.html"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(page)))
                self.end_headers()
                self.wfile.write(page)
            else:
                self._json({"error": "not found"}, 404)

        def do_POST(self):
            try:
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
                if self.path == "/api/ctl":
                    self._json(dash.control(body))
                elif self.path == "/api/send":
                    self._json(dash.send(body.get("user"), body.get("text", "")))
                else:
                    self._json({"error": "not found"}, 404)
            except Exception as exc:                          # noqa: BLE001
                self._json({"ok": False, "error": str(exc)}, 400)

    return H


def main():
    ap = argparse.ArgumentParser(description="Cluster dashboard")
    ap.add_argument("--spawn", action="store_true", help="start the node processes too")
    ap.add_argument("--algo", choices=["bully", "ring"], default="bully")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--no-browser", action="store_true")
    cfg.add_cluster_args(ap)
    args = ap.parse_args()
    cluster_cfg = cfg.load_cluster(args.config, args.nodes, args.base_port)
    mgr = None
    if args.spawn:
        mgr = Cluster(len(cluster_cfg), args.algo, args.base_port)
        mgr.start_all()
    dash = Dashboard(cluster_cfg, mgr, args.algo)
    srv = ThreadingHTTPServer(("0.0.0.0", args.port), make_handler(dash))
    srv.daemon_threads = True
    url = f"http://localhost:{args.port}"
    print(f"Dashboard: {url}   (Ctrl-C to stop{' and shut the nodes down' if mgr else ''})")
    if not args.no_browser:
        threading.Timer(1.5, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if mgr:
            mgr.stop_all()


if __name__ == "__main__":
    main()
