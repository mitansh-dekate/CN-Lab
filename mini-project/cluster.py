#!/usr/bin/env python3
"""cluster.py - starts, kills and restarts node processes (one OS process per node).

As a script it launches the whole cluster in the foreground:
    python cluster.py --nodes 5 --algo bully
"""
import argparse
import os
import signal
import subprocess
import sys
import time

import config as cfg

HERE = os.path.dirname(os.path.abspath(__file__))


class Cluster:
    def __init__(self, n=cfg.DEFAULT_NODES, algo="bully", base_port=cfg.BASE_PORT, log_dir=None):
        self.n, self.algo, self.base_port = n, algo, base_port
        self.log_dir = log_dir or os.path.join(HERE, "logs")
        os.makedirs(self.log_dir, exist_ok=True)
        self.procs = {}

    def start(self, node_id):
        if self.is_running(node_id):
            return
        out = open(os.path.join(self.log_dir, f"node{node_id}.log"), "a")
        self.procs[node_id] = subprocess.Popen(
            [sys.executable, "-u", os.path.join(HERE, "node.py"), "--id", str(node_id),
             "--nodes", str(self.n), "--algo", self.algo, "--base-port", str(self.base_port)],
            stdout=out, stderr=subprocess.STDOUT, cwd=HERE)

    def start_all(self):
        for i in range(1, self.n + 1):
            self.start(i)

    def kill(self, node_id):
        p = self.procs.get(node_id)
        if p and p.poll() is None:
            p.kill()                       # SIGKILL: a real, unannounced crash
            p.wait()

    def is_running(self, node_id):
        p = self.procs.get(node_id)
        return bool(p and p.poll() is None)

    def stop_all(self):
        for i in list(self.procs):
            self.kill(i)


def main():
    ap = argparse.ArgumentParser(description="Launch every node of the cluster")
    ap.add_argument("--algo", choices=["bully", "ring"], default="bully")
    ap.add_argument("--nodes", type=int, default=cfg.DEFAULT_NODES)
    ap.add_argument("--base-port", type=int, default=cfg.BASE_PORT)
    args = ap.parse_args()
    c = Cluster(args.nodes, args.algo, args.base_port)
    c.start_all()
    print(f"Started {args.nodes} nodes ({args.algo}). Logs in {c.log_dir}. Ctrl-C to stop.")
    try:
        signal.pause() if hasattr(signal, "pause") else time.sleep(10 ** 6)
    except KeyboardInterrupt:
        pass
    finally:
        c.stop_all()


if __name__ == "__main__":
    main()
