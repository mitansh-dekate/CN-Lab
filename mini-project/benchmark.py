#!/usr/bin/env python3
"""benchmark.py - Bully vs Ring election under a real leader crash.

For every (algorithm, cluster size, trial) it starts a fresh cluster, waits until the
highest-id node leads, crashes that leader with SIGKILL, and measures

    detect      crash -> first node starts an election (failure-detector delay)
    election    first election started -> every survivor has accepted the new leader
    failover    crash -> all survivors agree on the new, fully synced leader
    messages    election-related messages sent by all survivors (ELECTION, OK, RING token, COORDINATOR)

Results go to results/benchmark.csv and results/election_comparison.png (needs matplotlib).

    python benchmark.py --sizes 3 5 7 9 --trials 3
"""
import argparse
import csv
import json
import os
import statistics
import sys
import tempfile
import time
import urllib.request

import config as cfg
from cluster import Cluster

HERE = os.path.dirname(os.path.abspath(__file__))
ELECTION_KEYS = ("ELECTION", "ELECTION_OK", "RING_ELECTION", "COORDINATOR")


def status(cl_cfg, nid):
    host, port = cl_cfg[nid]
    try:
        with urllib.request.urlopen(f"http://{host}:{cfg.http_port(port)}/status", timeout=0.5) as r:
            return json.load(r)
    except Exception:                                      # noqa: BLE001
        return None


def ctl(cl_cfg, nid, **body):
    host, port = cl_cfg[nid]
    req = urllib.request.Request(f"http://{host}:{cfg.http_port(port)}/ctl", json.dumps(body).encode(),
                                 {"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=1).read()
    except Exception:                                      # noqa: BLE001
        pass


def wait(cond, timeout, step=0.05):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(step)
    return False


def agreed(cl_cfg, ids, leader):
    ss = [status(cl_cfg, i) for i in ids]
    if not all(ss):
        return False
    if any(s["leader"] != leader for s in ss):
        return False
    return next(s for s in ss if s["id"] == leader)["state"] == "LEADER" and next(s for s in ss if s["id"] == leader)["ready"]


def run_one(algo, n, base_port, log_dir):
    cl_cfg = cfg.load_cluster(None, n, base_port)
    ids = list(range(1, n + 1))
    cl = Cluster(n, algo, base_port, log_dir)
    cl.start_all()
    try:
        if not wait(lambda: agreed(cl_cfg, ids, n), 30):
            return None
        time.sleep(1.0)                                    # let start-up churn settle
        for i in ids:
            ctl(cl_cfg, i, cmd="reset_metrics")
        t0 = time.time()
        cl.kill(n)
        alive = ids[:-1]
        if not wait(lambda: agreed(cl_cfg, alive, n - 1), 40):
            return None
        t1 = time.time()
        ss = [status(cl_cfg, i) for i in alive]
        msgs = sum(s["counts"].get(k, 0) for s in ss for k in ELECTION_KEYS)
        starts = [e["t"] for s in ss for e in s["events"] if e["text"].startswith("ELECTION started") and e["t"] >= t0]
        new_leader = n - 1
        decided = [e["t"] for s in ss for e in s["events"] if e["t"] >= t0 and
                   (e["text"].startswith("became LEADER") or e["text"].startswith(f"accepted N{new_leader} as LEADER"))]
        if not starts or not decided:
            return None
        return {"detect": min(starts) - t0, "election": max(decided) - min(starts),
                "failover": t1 - t0, "messages": msgs}
    finally:
        cl.stop_all()
        time.sleep(0.3)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", type=int, nargs="+", default=[3, 5, 7, 9])
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--base-port", type=int, default=9000)
    ap.add_argument("--plot-only", action="store_true", help="redraw the graph from results/benchmark.csv")
    a = ap.parse_args()
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    log_dir = tempfile.mkdtemp(prefix="dchat_bench_")
    rows, run = [], 0
    csv_path = os.path.join(HERE, "results", "benchmark.csv")
    if a.plot_only:
        with open(csv_path) as f:
            for r in csv.DictReader(f):
                rows.append({"algo": r["algo"], "nodes": int(r["nodes"]), "trial": int(r["trial"]),
                             **{k: float(r[k]) for k in ("detect", "election", "failover", "messages")}})
        a.sizes = sorted({r["nodes"] for r in rows})
    for algo in (() if a.plot_only else ("bully", "ring")):
        for n in a.sizes:
            for t in range(1, a.trials + 1):
                run += 1
                r = run_one(algo, n, a.base_port + 30 * (run % 20), log_dir)
                if r is None:
                    print(f"{algo:5} n={n} trial {t}: FAILED (skipped)", flush=True)
                    continue
                rows.append({"algo": algo, "nodes": n, "trial": t, **{k: round(v, 3) for k, v in r.items()}})
                print(f"{algo:5} n={n} trial {t}: detect={r['detect']:.2f}s election={r['election']:.2f}s "
                      f"failover={r['failover']:.2f}s messages={r['messages']}", flush=True)
    path = csv_path
    if not a.plot_only:
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["algo", "nodes", "trial", "detect", "election", "failover", "messages"])
            w.writeheader()
            w.writerows(rows)

    def mean(algo, n, key):
        vals = [r[key] for r in rows if r["algo"] == algo and r["nodes"] == n]
        return statistics.mean(vals) if vals else float("nan")

    print("\nMean over trials")
    print(f"{'nodes':>5} | {'bully msgs':>10} {'ring msgs':>10} | {'bully election s':>16} {'ring election s':>15}")
    for n in a.sizes:
        print(f"{n:>5} | {mean('bully', n, 'messages'):>10.1f} {mean('ring', n, 'messages'):>10.1f} | "
              f"{mean('bully', n, 'election'):>16.3f} {mean('ring', n, 'election'):>15.3f}")
    print(f"\nCSV written to {path}")
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed: skipping the graph (pip install matplotlib)")
        return
    fig, ax = plt.subplots(1, 2, figsize=(11, 4.2))
    style = {"bully": ("#d89a00", "o", "Bully"), "ring": ("#1c7a8d", "s", "Ring (Chang-Roberts)")}
    for algo, (col, mk, label) in style.items():
        ax[0].plot(a.sizes, [mean(algo, n, "messages") for n in a.sizes], color=col, marker=mk, lw=2, label=label)
        ax[1].plot(a.sizes, [mean(algo, n, "failover") for n in a.sizes], color=col, marker=mk, lw=2, label=label)
    ax[0].set_title("Election messages after a leader crash")
    ax[0].set_ylabel("messages sent by survivors")
    ax[1].set_title("Recovery time after a leader crash")
    ax[1].set_ylabel("seconds (crash until a synced new leader)")
    ax[1].set_ylim(0, 3)
    for x in ax:
        x.set_xlabel("cluster size (nodes)")
        x.set_xticks(a.sizes)
        x.grid(alpha=.3)
        x.legend()
    fig.tight_layout()
    out = os.path.join(HERE, "results", "election_comparison.png")
    fig.savefig(out, dpi=150)
    print(f"Graph written to {out}")


if __name__ == "__main__":
    sys.exit(main())
