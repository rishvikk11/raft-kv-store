# raft-kv-store

A fault-tolerant, replicated key-value store written in Go from the [Raft paper](https://raft.github.io/raft.pdf). Three nodes hold identical copies of the data, agree on every change, and keep working when a node, even the leader, crashes. It runs as plain processes, in Docker Compose, and on Kubernetes (kind), and it comes with tests that break the cluster on purpose and then check every answer clients were given.

Standard library only. No third-party dependencies.

## What it does

- **Raft consensus:** leader election with randomized timeouts, log replication, the election restriction, and the rule that a leader only commits entries from its own term by counting replicas.
- **Durable state:** each node's term, vote and ledger go into a write-ahead log and are fsynced before the node answers any message, so a crashed node recovers what it had promised.
- **Linearizable reads:** the leader confirms it is still the leader with a majority (read-index) before it answers, so a leader that was cut off cannot return stale data.
- **Safe retries:** a client that sends `Client-Id` and `Request-Id` headers can retry a write without it being applied twice, even across a leader change or a full restart.
- **Leader step-down:** a leader that cannot hear a majority gives up leading.
- **Any node can serve any client:** followers pass requests on to the leader (`-forward`) or redirect.
- **Packaging:** a static image of about 5.7 MB that runs as a non-root user; a Compose cluster with one named volume per node; a Kubernetes StatefulSet with one disk per pod.

**Not implemented** (separate sections of the Raft paper): log compaction and snapshots, and cluster membership changes. The second matters: without it, a node that loses its disk cannot be safely replaced (see Limitations).

## How a write travels

```
client --HTTP--> any node --(forwarded)--> leader
                                             1. append to its ledger and fsync
                                             2. send to followers, who fsync too
                                             3. committed once a majority has it
                                             |
                every node applies committed entries, in order, to its own map
```

| File               | Its one job                                                                      |
| ------------------ | -------------------------------------------------------------------------------- |
| `kv/store.go`      | The data: a map with a lock                                                      |
| `kv/replicated.go` | HTTP front end: send writes through Raft, confirm reads, drop duplicate requests |
| `raft/raft.go`     | The Raft rules: elections, replication, commit, step-down, read confirmation     |
| `raft/wal.go`      | The write-ahead log: save before replying, replay on restart                     |
| `raft/http.go`     | Raft messages between nodes                                                      |

## Quick start

Needs Go 1.22 or newer (developed on 1.27.3). Docker Desktop for the Docker and Kubernetes parts; `kind` and `kubectl` for Kubernetes.

```
go test -race ./...        # 81 tests, about a minute
```

Three processes (one terminal each, from the repo root):

```
P=http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083
go run ./cmd/kvserver -id 0 -peers $P -data ./data/node0     # then -id 1 and -id 2

curl -s localhost:8081/raft/status                 # who is the leader?
curl -L -X PUT localhost:8082/kv/alice -d 42       # -L follows the redirect to the leader
curl -L localhost:8083/kv/alice
```

Docker and Kubernetes, each with a program that kills things while clients write and prints PASS or FAIL:

```
go run ./cmd/kvproof                               # builds and starts the Compose cluster, kills the leader container,
                                                   # replaces it, and checks nothing acknowledged was lost

bash scripts/k8s.sh up
go run ./cmd/kvproof -target k8s                   # delete a pod; Kubernetes recreates it, Raft rejoins it
go run ./cmd/kvproof -target k8s -scenario disk-loss
bash scripts/k8s.sh down
```

API: `PUT`, `GET` and `DELETE` on `/kv/{key}`; optional `Client-Id` and `Request-Id` headers for safe retries; `GET /raft/status` for a node's role, term and ledger.

## How it was tested

- **81 tests under the race detector:** each Raft rule on its own, a fake network that can cut, drop and delay messages, crash and restart with real log files, and a randomized chaos test.
- **Fault injection on a real cluster while clients keep writing:** kill the leader, cut the leader off from the others while clients can still reach it, crash every node at once. Afterwards a referee checks every answer ever given: no read older than an acknowledged write, no value from the future, no going backwards, every acknowledged write present, all replicas identical. The referee is itself tested against hand-made bad histories.
- **Mutation checking:** more than 20 deliberately broken versions (skip the election restriction, commit without a majority, answer reads without confirming leadership, forget the vote on restart, ...). Each one makes at least one test fail.
- **The proofs found flaws in the proofs:** several of my own checks passed without proving what they claimed (for example, counting writes made before a node had actually died). They were found by breaking the cluster on purpose and fixed; the story is in [PROGRESS.md](PROGRESS.md).

## Results

Apple M3 laptop, 3 nodes on one machine, real fsync, one 10 s run per cell, so differences under about 10% are noise.

|                                     | Measured                                                                    |
| ----------------------------------- | --------------------------------------------------------------------------- |
| Durable writes                      | about 230 per second from 8 clients up; median 8.95 ms for one client       |
| Reads (linearizable)                | 40,673 per second at 128 clients; median 0.14 ms for one client             |
| 90% reads, 10% writes               | 2,534 per second at 128 clients (write speed sets the limit)                |
| Same writes with fsync off (unsafe) | 36,576 per second, about 160 times faster: the price of durability          |
| Failover                            | a new leader found 272 ms after the leader's container was killed (one run) |

fsync on this Mac takes 2.52 ms but only 0.37 ms inside Docker's Linux VM, so the Kubernetes write numbers are not comparable to the ones above and carry a weaker guarantee against a power cut. All runs, the 5-node and Kubernetes results, and what each number does and does not mean are in [PROGRESS.md](PROGRESS.md).

## Design choices worth knowing

- **fsync before every reply, under one lock.** Simple and safe, and it caps durable writes at the speed measured above. Saving several waiting writes with one fsync (group commit) is the obvious next step.
- **Reads use read-index, not the log.** A read costs one round trip to a majority and no disk write.
- **Duplicates are dropped when entries are applied,** because that is the one place every node sees the same requests in the same order, so a new leader already knows what was done.
- **A StatefulSet, not a Deployment.** A Raft member's identity is its saved state, so its name and its disk must stay together when Kubernetes replaces its pod.

## Limitations

- No log compaction: the ledger only grows (over 126,000 entries after about an hour of testing).
- No membership changes. A node that loses its disk comes back blank under its old name and Raft cannot safely resurrect it. `TestNodeThatLosesItsDiskCanLoseAcknowledgedWrites` shows an acknowledged write being lost this way, and `TestSameScenarioWithTheDiskIntactKeepsTheWrite` shows the identical story with the disk kept.
- Exactly-once holds for a client that sends one request at a time per `Client-Id`.
- `docker kill` and pod deletion test process crashes, not power cuts. The Kubernetes cluster has one machine, so it shows pod and disk handling, not surviving the loss of a machine.
- All benchmarks run on one laptop with no real network between nodes.

## Repository layout

```
cmd/kvserver   the server (standalone or one node of a cluster)
cmd/kvbench    throughput and latency tool that checks its own results
cmd/kvproof    kill-and-recover experiment for Docker and Kubernetes
kv/            store, HTTP front end, and the fault-injection tests
raft/          the Raft node, its write-ahead log, and its tests
deploy/k8s/    kind cluster config and Kubernetes manifests
scripts/       benchmark and cluster helper scripts
Dockerfile, docker-compose.yml
PROGRESS.md    the engineering log: tradeoffs, every measured number, what was and was not verified
```

## Reference

Diego Ongaro and John Ousterhout, _In Search of an Understandable Consensus Algorithm_ (2014). Built from the paper and the public Go, Docker and Kubernetes documentation.
