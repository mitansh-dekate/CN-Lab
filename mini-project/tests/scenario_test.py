#!/usr/bin/env python3
"""Automated end-to-end scenarios. Exit code 0 only if every check passes.

    python tests/scenario_test.py [--algo bully|ring] [--nodes 5]

Scenarios
  1. startup: the highest id wins the first election
  2. normal chat: two clients on different nodes see the same total order
  3. leader crash during traffic: new leader elected, no message lost or duplicated
  4. crashed leader restarts: it takes over again (Bully) and recovers the whole history
  5. network partition of the leader: majority elects a new one; after healing all logs converge
"""
import argparse
import json
import os
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config as cfg                      # noqa: E402
from client import ChatClient             # noqa: E402
from cluster import Cluster               # noqa: E402

FAILED = []


def check(name, ok, detail=""):
    print(("  PASS  " if ok else "  FAIL  ") + name + (f"  [{detail}]" if detail else ""), flush=True)
    if not ok:
        FAILED.append(name)


def status(cluster_cfg, nid):
    host, port = cluster_cfg[nid]
    try:
        with urllib.request.urlopen(f"http://{host}:{cfg.http_port(port)}/status", timeout=0.6) as r:
            return json.load(r)
    except Exception:                                   # noqa: BLE001
        return None


def ctl(cluster_cfg, nid, **body):
    host, port = cluster_cfg[nid]
    req = urllib.request.Request(f"http://{host}:{cfg.http_port(port)}/ctl", json.dumps(body).encode(),
                                 {"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=1).read()
    except Exception:                                   # noqa: BLE001
        pass


def wait(cond, timeout, step=0.1):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(step)
    return False


def leaders(cluster_cfg, ids):
    out = {}
    for i in ids:
        s = status(cluster_cfg, i)
        out[i] = (s["leader"], s["state"], s["ready"]) if s else None
    return out


def agreed_leader(cluster_cfg, ids, expect):
    ls = leaders(cluster_cfg, ids)
    return all(v and v[0] == expect for v in ls.values()) and ls[expect][1] == "LEADER" and ls[expect][2]


def converged(cluster_cfg, ids, total):
    ss = [status(cluster_cfg, i) for i in ids]
    return all(s and s["commit"] == total for s in ss) and len({s["digest"] for s in ss}) == 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--algo", default="bully")
    ap.add_argument("--nodes", type=int, default=5)
    ap.add_argument("--base-port", type=int, default=9300)
    a = ap.parse_args()
    n = a.nodes
    cl_cfg = cfg.load_cluster(None, n, a.base_port)
    all_ids = list(range(1, n + 1))
    top = n
    cl = Cluster(n, a.algo, a.base_port)
    for f in os.listdir(cl.log_dir):
        if f.startswith("node"):
            os.remove(os.path.join(cl.log_dir, f))
    cl.start_all()
    clients = []
    try:
        print(f"\n== 1. startup ({a.algo}, {n} nodes)")
        check(f"all nodes agree node {top} is leader", wait(lambda: agreed_leader(cl_cfg, all_ids, top), 15))

        print("\n== 2. normal chat with two clients on different nodes")
        alice = ChatClient("alice", cl_cfg, prefer=1).start()
        bob = ChatClient("bob", cl_cfg, prefer=2).start()
        clients = [alice, bob]
        wait(lambda: alice.connected and bob.connected, 5)
        for i in range(10):
            (alice if i % 2 == 0 else bob).send(f"hello {i}")
        check("both clients receive all 10 messages",
              wait(lambda: len(alice.delivered) == 10 and len(bob.delivered) == 10, 8),
              f"alice={len(alice.delivered)} bob={len(bob.delivered)}")
        check("identical total order on both clients",
              [e["id"] for e in alice.delivered] == [e["id"] for e in bob.delivered])
        check("sequence numbers are gapless", [e["seq"] for e in alice.delivered] == list(range(1, 11)))

        print(f"\n== 3. crash the leader (node {top}) while clients keep typing")
        t_kill = time.time()
        cl.kill(top)
        for i in range(10, 20):
            (alice if i % 2 == 0 else bob).send(f"during failover {i}")
            time.sleep(0.15)
        new_leader = top - 1
        ok = wait(lambda: agreed_leader(cl_cfg, [i for i in all_ids if i != top], new_leader), 20)
        check(f"node {new_leader} elected after the crash", ok, f"{time.time() - t_kill:.1f}s after kill")
        check("no message lost across the failover (20 total on both clients)",
              wait(lambda: len(alice.delivered) == 20 and len(bob.delivered) == 20, 15),
              f"alice={len(alice.delivered)} bob={len(bob.delivered)}")
        ids_a = [e["id"] for e in alice.delivered]
        check("no duplicates", len(set(ids_a)) == len(ids_a))
        check("same order on both clients", ids_a == [e["id"] for e in bob.delivered])
        check("every live node holds the same committed log",
              wait(lambda: converged(cl_cfg, [i for i in all_ids if i != top], 20), 8))

        print(f"\n== 4. restart node {top} (empty log) - it must recover everything")
        cl.start(top)
        if a.algo == "bully":
            check(f"node {top} takes leadership back and everyone follows",
                  wait(lambda: agreed_leader(cl_cfg, all_ids, top), 20))
        else:
            check("cluster re-converges on one leader", wait(lambda: len({v[0] for v in leaders(cl_cfg, all_ids).values() if v}) == 1, 20))
        bob.send("after restart A")
        alice.send("after restart B")
        check("22 messages committed on every node, identical logs",
              wait(lambda: converged(cl_cfg, all_ids, 22), 15))
        check("clients received all 22", wait(lambda: len(alice.delivered) == 22 and len(bob.delivered) == 22, 10))

        print("\n== 5. network partition of the current leader")
        cur = max(i for i in all_ids if (status(cl_cfg, i) or {}).get("state") == "LEADER")
        ctl(cl_cfg, cur, cmd="isolate")
        rest = [i for i in all_ids if i != cur]
        exp = max(rest)
        check(f"majority side elects node {exp}", wait(lambda: agreed_leader(cl_cfg, rest, exp), 20))
        for i in range(3):
            alice.send(f"during partition {i}")
        check("majority keeps committing during the partition",
              wait(lambda: converged(cl_cfg, rest, 25), 15))
        ctl(cl_cfg, cur, cmd="heal")
        check("after healing, one leader and identical logs everywhere",
              wait(lambda: converged(cl_cfg, all_ids, 25), 25))
        check("clients still see a gapless, duplicate-free stream",
              wait(lambda: [e["seq"] for e in alice.delivered] == list(range(1, 26)), 10),
              f"last_seq={alice.last_seq}")
    finally:
        for c in clients:
            c.stop()
        cl.stop_all()
    print("\n" + ("ALL CHECKS PASSED" if not FAILED else f"{len(FAILED)} CHECK(S) FAILED: {FAILED}"))
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main()
