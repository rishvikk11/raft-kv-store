# Fault-Tolerant Replicated KV Store (Go)

A key-value store replicated across 3 nodes with Raft, later run in Docker and local Kubernetes.
Implemented from the Raft paper (raft.github.io/raft.pdf, mainly Figure 2) and public Go/Docker/Kubernetes docs only.

## Status

- [x] Stage 1: single-server store (in-memory store + HTTP interface, race-clean)
- [x] Stage 2: Raft consensus (election, replication, safety rules, durable state, wired into the HTTP server)
- [x] Stage 3: leader step-down, linearizable reads (read-index), request IDs for safe retries
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
    (raft.go also holds ConfirmRead: the read-index recipe, and the leader's step-down check)
    raft/*_test.go       harness with a fake network + election, replication, rules, persistence, chaos tests
    kv/*_test.go         unit tests + end-to-end 3-node cluster tests (real sockets, real disk)
    cmd/kvserver         runs standalone, or as one node of a cluster

## Architecture

    client --HTTP--> any node --(307 redirect)--> leader
    leader: Propose(cmd) -> append to ledger + fsync -> replicate to followers
            -> committed once stored on a majority -> every node applies it to its Store
    Raft messages (RequestVote, AppendEntries) travel between nodes as HTTP POSTs.

Writes are acknowledged only after the entry is committed AND applied on the node that took the request.
Reads: the leader first calls ConfirmRead (see Design decisions), then answers from its local Store.
Retries: a request may carry Client-Id and Request-Id headers; duplicates are filtered when entries are APPLIED, so every node decides identically.

## Design decisions

| Decision                                                                                                                                                                                                              | Why                                                                                                                                  | Tradeoff                                                                                                              |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------- |
| `sync.RWMutex` around one map (Stage 1)                                                                                                                                                                               | Simple, obviously correct                                                                                                            | One lock for the whole map                                                                                            |
| HTTP/JSON for both clients and Raft RPCs                                                                                                                                                                              | Inspectable with curl, one port per node                                                                                             | More overhead than a binary protocol                                                                                  |
| One mutex guards all Raft state; fsync happens while holding it                                                                                                                                                       | Easiest to reason about; the reply never leaves before the data is on disk                                                           | Disk latency blocks other handlers; first thing to revisit if Stage 4 measurements demand it                          |
| Append-only WAL with CRC frames: hard-state, entry, truncate records                                                                                                                                                  | Cheap appends; a half-written final record after a crash is detected and dropped                                                     | File only grows (no log compaction); mid-file corruption is a hard error rather than silent data loss                 |
| Leader appends a no-op entry on election                                                                                                                                                                              | Lets entries from earlier terms commit (Figure 8 rule) and gives Stage 3 reads a current-term anchor                                 | One extra ledger entry per election                                                                                   |
| Commit only counts replicas of CURRENT-term entries                                                                                                                                                                   | Figure 8: old-term entries can be overwritten even when a majority holds them                                                        | Old entries commit indirectly via the no-op                                                                           |
| Follower replies with ConflictIndex so the leader skips back quickly                                                                                                                                                  | Avoids stepping back one entry per round trip                                                                                        | Slightly more logic than the paper's basic version                                                                    |
| Randomized election timeout 300-600ms, heartbeat 75ms (defaults)                                                                                                                                                      | Avoids split votes; heartbeat much shorter than timeout                                                                              | Failover takes roughly one timeout                                                                                    |
| Persist errors panic                                                                                                                                                                                                  | A node that cannot save its promises must not keep running                                                                           | A full disk crashes the node                                                                                          |
| Apply callback runs on one goroutine, in order, outside the lock                                                                                                                                                      | State machine sees every committed entry exactly once, in order                                                                      | Slow Apply delays later entries                                                                                       |
| Leader steps down if it has not heard from a majority within one minimum election timeout (check-quorum)                                                                                                              | A cut-off leader stops accepting writes and stops claiming leadership at about the time a replacement can be elected                 | A leader can step down needlessly if replies are delayed or lost for a whole timeout; the cost is just a new election |
| Reads use read-index (ConfirmRead): wait for a commit in the leader's own term, note the commit index, get a majority to answer a heartbeat sent AFTER the read arrived, wait for the Store to apply up to that index | Linearizable reads without writing every read into the log; does not depend on clocks                                                | Each read costs a round trip to a majority; a leader that cannot reach a majority refuses (503) instead of answering  |
| Only replies to heartbeats built after the read count as confirmation                                                                                                                                                 | A reply to an older heartbeat only proves we led at some earlier moment                                                              | Slightly more bookkeeping (a counter on each heartbeat)                                                               |
| Request IDs = Client-Id + a per-client counter (Request-Id 1, 2, 3...); the cluster remembers each client's last request number and its answer                                                                        | Bounded memory, simple to explain, and a retry is recognised even by a new leader because the memory is part of the replicated state | One outstanding request per client ID; the table grows with the number of clients and never expires                   |
| Duplicates are filtered at APPLY time, not when the request arrives                                                                                                                                                   | Apply is the one place every node sees the same requests in the same order                                                           | A retry still takes a (cheap) slot in the ledger                                                                      |
| Request IDs are optional                                                                                                                                                                                              | Easy to use with plain curl                                                                                                          | Without them a retried write can be applied twice (a test shows this on purpose)                                      |

## Not implemented (separate sections of the Raft paper)

- Log compaction / snapshots (paper section 7): the WAL grows forever.
- Cluster membership changes (paper section 6): cluster size is fixed at startup.

## Known limitations

- Exactly-once holds only for a client that sends one request at a time per Client-Id and re-sends the same Request-Id when retrying. Concurrent requests under one Client-Id can be rejected as stale (409).
- Client sessions never expire, so that table only grows.
- Step-down is timer-based, so a paused leader can briefly hold a stale belief; that is why reads do NOT rely on it and confirm with a majority every time. A leader can also step down needlessly when replies are lost for a whole timeout; the cost is one extra election.
- A write that times out (503) may or may not have been applied; with a Request-Id it is safe to retry.

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

- `go vet ./...` clean; `go test -race ./...`: 59 tests pass (kv: 23, raft: 36).
- Repeated 8x in full and the chaos test 10x with random fault schedules: no failures, no data races.
- Mutation check: each of these deliberate bugs makes at least one test fail: no election restriction, committing old-term entries by counting, committing without a majority, not persisting the vote, not persisting the log, accepting a stale leader's messages, truncating the log on a duplicate message.
- Stage 3 step-down: new tests (cut-off leader steps down and refuses writes; only one leader claimed after a partition; leader with a majority keeps leading) pass. Breaking the check on purpose (never step down; step down unless hearing everyone) makes them fail. Full suite repeated 5x: no failures.
- Stage 3 reads: a test reproduces the bug on purpose (a cut-off leader answering from its own data returns balance=100 after balance=150 was committed) and the safe read refuses instead. A churn test (3 readers, a writer, the leader cut off mid-run) saw tens of thousands of confirmed reads and no stale answer. White-box tests control which heartbeat reply arrives when. Breaking each step of the read recipe on purpose (skip the current-term commit wait; skip the majority check; accept replies to older heartbeats; skip waiting for the Store; skip the leader check) makes a test fail every time.
- Stage 3 retries: a retried write no longer overwrites a newer value written by someone else; a retried delete repeats its original answer; this holds after the leader is killed and after a full restart. Breaking the dedupe three ways makes tests fail.
- Full suite repeated 4x, chaos / read-churn / lossy-network tests 6x more: no failures, no data races.
- Manual (3 processes): A's request 1, B's write, A's retry of request 1 left the value as B's; the same repeat without IDs overwrote it. A read after `kill -9` of the leader worked through the new leader.
- Manual (Stage 2): 3 separate processes with real fsync. Wrote through a follower, `kill -9` the leader, wrote and read during the outage, restarted the leader from disk; it rejoined as follower with commitIndex = lastApplied = 4.

## Measured numbers

None yet. Only numbers we actually measure get recorded here (Stage 4).
