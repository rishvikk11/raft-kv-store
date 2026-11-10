# Fault-Tolerant Replicated KV Store (Go)

A key-value store replicated across 3 nodes with Raft, later run in Docker and local Kubernetes.
Implemented from the Raft paper (raft.github.io/raft.pdf, mainly Figure 2) and public Go/Docker/Kubernetes docs only.

## Status

- [x] Stage 1: single-server store (in-memory store + HTTP interface, race-clean)
- [x] Stage 2: Raft consensus (election, replication, safety rules, durable state, wired into the HTTP server)
- [ ] Stage 3: linearizable reads, request IDs, leader step-down
- [ ] Stage 4: fault-injection tests and measurements
- [ ] Stage 5: Docker / Compose
- [ ] Stage 6: local Kubernetes

## Layout

    kv/store.go          the state machine: map + RWMutex (Get, Put, Delete)
    kv/server.go         standalone HTTP handler (Stage 1, no replication)
    kv/replicated.go     HTTP server whose writes go through Raft before touching the store
    raft/raft.go         the Raft node: election, replication, commit rules (Figure 2)
    raft/wal.go          write-ahead log: durable term, vote and ledger
    raft/http.go         Raft messages over HTTP/JSON between nodes (+ /raft/status)
    raft/*_test.go       harness with a fake network + election, replication, rules, persistence, chaos tests
    kv/*_test.go         unit tests + end-to-end 3-node cluster tests (real sockets, real disk)
    cmd/kvserver         runs standalone, or as one node of a cluster

## Architecture

    client --HTTP--> any node --(307 redirect)--> leader
    leader: Propose(cmd) -> append to ledger + fsync -> replicate to followers
            -> committed once stored on a majority -> every node applies it to its Store
    Raft messages (RequestVote, AppendEntries) travel between nodes as HTTP POSTs.

Writes are acknowledged only after the entry is committed AND applied on the node that took the request.
A write that times out may or may not have been applied (the retry problem; Stage 3 adds request IDs).

## Design decisions

| Decision                                                             | Why                                                                                                  | Tradeoff                                                                                              |
| -------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `sync.RWMutex` around one map (Stage 1)                              | Simple, obviously correct                                                                            | One lock for the whole map                                                                            |
| HTTP/JSON for both clients and Raft RPCs                             | Inspectable with curl, one port per node                                                             | More overhead than a binary protocol                                                                  |
| One mutex guards all Raft state; fsync happens while holding it      | Easiest to reason about; the reply never leaves before the data is on disk                           | Disk latency blocks other handlers; first thing to revisit if Stage 4 measurements demand it          |
| Append-only WAL with CRC frames: hard-state, entry, truncate records | Cheap appends; a half-written final record after a crash is detected and dropped                     | File only grows (no log compaction); mid-file corruption is a hard error rather than silent data loss |
| Leader appends a no-op entry on election                             | Lets entries from earlier terms commit (Figure 8 rule) and gives Stage 3 reads a current-term anchor | One extra ledger entry per election                                                                   |
| Commit only counts replicas of CURRENT-term entries                  | Figure 8: old-term entries can be overwritten even when a majority holds them                        | Old entries commit indirectly via the no-op                                                           |
| Follower replies with ConflictIndex so the leader skips back quickly | Avoids stepping back one entry per round trip                                                        | Slightly more logic than the paper's basic version                                                    |
| Randomized election timeout 300-600ms, heartbeat 75ms (defaults)     | Avoids split votes; heartbeat much shorter than timeout                                              | Failover takes roughly one timeout                                                                    |
| Persist errors panic                                                 | A node that cannot save its promises must not keep running                                           | A full disk crashes the node                                                                          |
| Apply callback runs on one goroutine, in order, outside the lock     | State machine sees every committed entry exactly once, in order                                      | Slow Apply delays later entries                                                                       |
| GET in Stage 2 is a NAIVE leader-local read                          | Keeps Stage 2 focused; fixed in Stage 3                                                              | Can return stale data (see Known limitations)                                                         |

## Not implemented (separate sections of the Raft paper)

- Log compaction / snapshots (paper section 7): the WAL grows forever.
- Cluster membership changes (paper section 6): cluster size is fixed at startup.

## Known limitations (to fix in later stages)

- Reads are not linearizable: a leader cut off from the majority, or one that has not yet applied the latest commits, can answer a GET with stale data. (Stage 3)
- A leader that loses contact with a majority does not step down on its own. It steps down only when it hears a higher term. (Stage 3)
- A client that times out and retries a write may apply it twice. (Stage 3: unique request IDs)

## How to run the tests

    go vet ./...
    go test -race -count=1 ./...
    CHAOS_SEED=<n> go test -race -run TestChaos -v ./raft/    # replay a chaos fault schedule

## How to run a 3-node cluster (3 terminals, from the repo root)

    P=http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083
    go run ./cmd/kvserver -id 0 -peers $P -data ./data/node0
    go run ./cmd/kvserver -id 1 -peers $P -data ./data/node1
    go run ./cmd/kvserver -id 2 -peers $P -data ./data/node2

    curl -s localhost:8081/raft/status                 # who is leader?
    curl -L -X PUT localhost:8082/kv/alice -d 42       # -L follows the redirect to the leader
    curl -L localhost:8083/kv/alice

Ctrl+C or `kill -9` the leader, then keep using the other nodes; restart it with the same command and `-data` directory.
Add `data/` to .gitignore.

## Current test results (Go 1.22.2, race detector on)

- `go vet ./...` clean; `go test -race ./...`: 40 tests pass (kv: 17, raft: 23).
- Repeated 8x in full and the chaos test 10x with random fault schedules: no failures, no data races.
- Mutation check: each of these deliberate bugs makes at least one test fail: no election restriction, committing old-term entries by counting, committing without a majority, not persisting the vote, not persisting the log, accepting a stale leader's messages, truncating the log on a duplicate message.
- Manual: 3 separate processes with real fsync. Wrote through a follower, `kill -9` the leader, wrote and read during the outage, restarted the leader from disk; it rejoined as follower with commitIndex = lastApplied = 4.

## Measured numbers

None yet. Only numbers we actually measure get recorded here (Stage 4).
