# Distributed Chat with Leader Election

Computer Networks mini project. A chat service made of several server nodes. If the
coordinator (leader) crashes, the remaining servers detect it, elect a new leader and
carry on, with no message lost, duplicated or reordered.

Pure Python 3 standard library (matplotlib is only needed for the benchmark graph).
No hardware, no Docker.

## Quick start

```bash
cd dchat
python dashboard.py --spawn            # starts 5 nodes + dashboard, opens http://localhost:8080
python client.py --user alice          # terminal chat client (open several in other terminals)
python client.py --user bob --node 2
```

Other useful commands:

```bash
python dashboard.py --spawn --algo ring      # use the Ring (Chang-Roberts) election instead of Bully
python cluster.py --nodes 5                  # nodes only, no dashboard
python node.py --id 3 --nodes 5              # one node by hand (run one per terminal)
python tests/scenario_test.py                # automated crash / restart / partition test
python benchmark.py --sizes 3 5 7 9 --trials 3   # Bully vs Ring graphs in results/
```

Multi-laptop run: write a `cluster.json` (see `config.py`) with each laptop's IP, start
`python node.py --id N --config cluster.json` on each, and `python dashboard.py --config cluster.json`
on any one. Allow the ports through the firewall (node port `9000+id`, status port `9100+id`).

## Architecture

```
 clients (any node)            +-----------+     heartbeats, election, replication
 alice ----------------------> |  Node 1   | <---------------------------------+
 bob   ----------------------> |  Node 2   | <------------+                     |
                               |  Node 3   |              |   one OS process    |
 dashboard (web) ------------> |  Node 4   |              |   per node,         |
   polls /status, injects      |  Node 5*  | leader       |   plain TCP sockets |
   faults via /ctl             +-----------+ ------------>+---------------------+
```

| File | Role |
|---|---|
| `node.py` | The server: sockets, heartbeats, Bully + Ring election, replication, sync, fault injection, HTTP status API |
| `client.py` | Chat client with auto-reconnect, resume and idempotent retries |
| `dashboard.py`, `dashboard.html` | Live console: cluster ring, events, chat, failure buttons, election cost |
| `cluster.py` | Starts, kills (SIGKILL) and restarts node processes |
| `protocol.py`, `config.py` | Wire format, Lamport clock, cluster layout and timing constants |
| `tests/scenario_test.py` | End-to-end checks: failover, restart, partition |
| `benchmark.py` | Bully vs Ring: messages and time against cluster size |

### How each requirement is met

* **Heartbeat failure detection.** Every node sends a HEARTBEAT to every peer each 0.5 s. A peer silent for 1.5 s is presumed dead.
* **Leader election.** Bully by default (highest live id wins). `--algo ring` switches to Chang-Roberts ring election. Both announce with COORDINATOR.
* **Terms.** Each leader has a term number. A leader with an older term is rejected, so a stale leader returning after a partition cannot overwrite newer history.
* **Ordering.** Only the leader numbers messages (1, 2, 3, ...). That gives one total order seen identically by all clients. Every entry also carries a Lamport timestamp, shown in the dashboard.
* **Replication, no lost messages.** The leader sends each entry to all followers. An entry is committed only when a majority holds it, and only committed entries are shown to clients. Any majority contains at least one node with every committed entry.
* **New-leader catch-up.** A new leader first collects the logs of the live nodes and adopts the most complete one before it accepts writes.
* **Rejoin / history sync.** A restarted node starts empty and receives the missing history from the leader (SYNC_REQ / SYNC_RESP). Divergent uncommitted entries are replaced.
* **Client failover.** A client that loses its node, or whose node goes silent or loses the cluster, reconnects to another node, resumes from the last sequence number it saw and re-sends unacknowledged messages. The cluster de-duplicates by message id.
* **Failure injection.** Crash (real SIGKILL), cut a node off the network (partition), add latency, add packet loss. All from the dashboard.
* **Quorum behaviour.** Without a majority alive the cluster cannot commit, so it pauses writes rather than risk split history (consistency over availability).

### Message types

`HEARTBEAT, ELECTION, ELECTION_OK, RING_ELECTION, COORDINATOR, LOG_QUERY, LOG_REPLY, FORWARD, REPLICATE, ACK, COMMIT, SYNC_REQ, SYNC_RESP` between nodes;
`HELLO, CHAT, DELIVER, INFO` between client and node. All are one JSON object per line over TCP (see `protocol.py`).

## Demo script (about 5 minutes)

1. `python dashboard.py --spawn`. Point out the five nodes and the crown on node 5 (highest id wins under Bully).
2. Click **Send 10** and show the chat panel filling, with identical fingerprints on all nodes.
3. Click **Send 10** again and immediately press **Crash current leader**. Show the violet election pulse, node 4 taking the crown within about 2 s, and all 20 messages still arriving in order.
4. Select node 5 and press **Restart node**. It rejoins, takes leadership back (Bully) and its log fingerprint matches the others.
5. Select the leader and press **Cut network**. Show the majority electing a new leader while the isolated node is marked. Send messages, then press **Restore network** and show everything converging.
6. Apply 500 ms latency and 30% packet loss to a node to show the cluster staying correct under a bad network.
7. Show `python tests/scenario_test.py` printing all PASS lines, and the Bully vs Ring graph from `results/`.

## Known limitations (state these honestly in the report)

* Logs live in memory. If every node stops at once, history is lost (a disk-backed log is the natural extension).
* Bully elects by highest id, not by most up-to-date log, so the new leader repairs itself by pulling the best log from its peers before serving, instead of using Raft's vote restriction.
* Cluster membership is fixed at start-up. There is no TLS or authentication, since the project focuses on the networking and distributed-systems behaviour.
