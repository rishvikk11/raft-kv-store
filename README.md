# Fault-Tolerant Replicated KV Store (Go)

A key-value store replicated across 3 nodes with Raft, later run in Docker and local Kubernetes.
Implemented from the Raft paper (raft.github.io/raft.pdf, mainly Figure 2) and public Go/Docker/Kubernetes docs only.

## Status

- [x] Stage 1: single-server store (in-memory store + HTTP interface, race-clean)
- [x] Stage 2: Raft consensus (election, replication, safety rules, durable state, wired into the HTTP server)
- [x] Stage 3: leader step-down, linearizable reads (read-index), request IDs for safe retries
- [x] Stage 4: fault-injection tests, self-checking measuring tool, and measured numbers on the author's laptop
- [x] Stage 5: Docker / Compose: image, 3-node cluster with one volume each, and a kill-and-recover proof (13 checks) all verified on the author's machine
- [ ] Stage 6: local Kubernetes

## Layout

    cmd/kvserver/main.go   how to start the program (flags; standalone or cluster node)
    kv/store.go            the data: a map + RWMutex (Get, Put, Delete)
    kv/server.go           standalone HTTP handler (Stage 1, no replication)
    kv/replicated.go       cluster HTTP handler: writes go through Raft, safe reads, request-ID dedupe
    raft/raft.go           the Raft rules: election, replication, commit, step-down, ConfirmRead
    raft/wal.go            write-ahead log: durable term, vote and ledger
    raft/http.go           Raft messages over HTTP/JSON between nodes (+ /raft/status)
    cmd/kvbench/main.go    load generator: measures throughput and latency, then checks its own work
    scripts/bench.sh       one command: builds, starts N nodes, runs kvbench, saves results
    Dockerfile             multi-stage build: compile in a Go image, ship only the program in an empty image
    docker-compose.yml     3 nodes, one container and one named volume each
    cmd/kvproof/main.go    runs the kill-and-recover experiment against the Docker cluster; prints PASS/FAIL
    *_test.go              see "Codebase map" below

## Codebase map

Every non-test file answers one question.

| File                          | Its one job                                                                                                                      |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `kv/store.go`                 | Hold key-value pairs safely under many goroutines                                                                                |
| `kv/server.go`                | Single-server HTTP onto the Store, no Raft                                                                                       |
| `kv/replicated.go`            | Turn a write into a Raft command, wait until it is committed, then apply it to the Store; confirm reads; drop duplicate requests |
| `raft/raft.go`                | Decide who leads, copy the ledger, decide what is committed, step down, confirm reads                                            |
| `raft/wal.go`                 | Save term, vote and ledger to disk before any reply; replay on restart                                                           |
| `raft/http.go`                | Carry Raft messages between nodes; serve /raft/status                                                                            |
| `cmd/kvserver/main.go`        | Parse flags, build the pieces, start the server                                                                                  |
| `cmd/kvbench/main.go`         | Send load to the leader from many clients, record every latency, read back what was written to prove the run was valid           |
| `scripts/bench.sh`            | Build, start 3 or 5 nodes with real fsync, run kvbench, save the results                                                         |
| `Dockerfile`, `.dockerignore` | Build a tiny image containing only the server program                                                                            |
| `docker-compose.yml`          | Run 3 nodes, one container each, each with its own volume holding its Raft state                                                 |
| `cmd/kvproof/main.go`         | Kill a container mid-traffic, restart it, replace it, and check nothing acknowledged was lost                                    |

**Who knows about whom.** `store.go` knows nothing. `raft/` knows nothing about keys or values; it talks to the outside through an Apply callback (committed commands come out) and a Transport (messages go to other nodes). `replicated.go` is the only file that knows both Raft and the Store. `wal.go` is used only by `raft.go`.

**One PUT through the files.**

1. `main.go` started a server whose handler comes from `replicated.go`.
2. `handlePut` reads the request and calls `submit`.
3. `submit` calls `Propose` (raft.go): the leader appends a ledger line and `persistLocked` writes it to disk via wal.go.
4. A `replicate` goroutine (raft.go) sends it to each follower through http.go; each follower runs `HandleAppendEntries` and saves it to its own disk.
5. Once a majority has it, `advanceCommitLocked` marks it committed.
6. The `applier` goroutine passes it through the Apply callback to `apply` (replicated.go), which checks the request ID and calls `Put` on the Store.
7. `apply` wakes the waiting handler; the client gets 204.

A GET is shorter: `handleGet` calls `ConfirmRead` (raft.go), then `Get` on the Store.

**Reading raft.go top to bottom:** message types, then `Config` and `Node`; the public doors (`NewNode`, `Start`, `Stop`, `Status`, `Propose`); background loops (`ticker`, `applier`); elections (`startElectionLocked`, `becomeLeaderLocked`, `HandleRequestVote`); step-down and reads (`checkQuorumLocked`, `ConfirmRead`); copying the ledger (`replicate`, `advanceCommitLocked`, `HandleAppendEntries`). A function ending in `Locked` must be called while holding the node's mutex.

**Tests, by what they pretend.**

| File                                          | What it checks                                                               |
| --------------------------------------------- | ---------------------------------------------------------------------------- |
| `raft/harness_test.go`                        | Not tests: the fake network and cluster helpers the other raft tests use     |
| `raft/election_test.go`                       | One leader appears; a new one after a failure                                |
| `raft/replication_test.go`                    | Ledgers match; conflicting entries are overwritten; lossy network            |
| `raft/rules_test.go`                          | Individual Raft rules, no network                                            |
| `raft/persistence_test.go`                    | Crash and restart; the randomized chaos test                                 |
| `raft/wal_test.go`                            | The disk file: half-written and damaged records                              |
| `raft/stepdown_test.go`                       | A cut-off leader gives up                                                    |
| `raft/read_test.go`, `raft/readindex_test.go` | Stale reads vs safe reads; each step of read-index                           |
| `kv/store_test.go`, `kv/server_test.go`       | Stage 1 basics                                                               |
| `kv/replicated_test.go`                       | Three real servers on real ports, real disks                                 |
| `kv/dedupe_test.go`                           | Retried requests are not applied twice                                       |
| `kv/redirect_test.go`                         | Followers redirect clients to the client-facing address, not the private one |
| `kv/history_test.go`, `kv/faults_test.go`     | Stage 4: fault injection under live traffic and the correctness checker      |

## Architecture

    client --HTTP--> any node --(307 redirect)--> leader
    leader: Propose(cmd) -> append to ledger + fsync -> replicate to followers
            -> committed once stored on a majority -> every node applies it to its Store
    Raft messages (RequestVote, AppendEntries) travel between nodes as HTTP POSTs.

Writes are acknowledged only after the entry is committed AND applied on the node that took the request.
Reads: the leader first calls ConfirmRead (see Design decisions), then answers from its local Store.
Retries: a request may carry Client-Id and Request-Id headers; duplicates are filtered when entries are APPLIED, so every node decides identically.

## Design decisions

| Decision                                                                                                                                                                                                              | Why                                                                                                                                  | Tradeoff                                                                                                                    |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| `sync.RWMutex` around one map (Stage 1)                                                                                                                                                                               | Simple, obviously correct                                                                                                            | One lock for the whole map                                                                                                  |
| HTTP/JSON for both clients and Raft RPCs                                                                                                                                                                              | Inspectable with curl, one port per node                                                                                             | More overhead than a binary protocol                                                                                        |
| One mutex guards all Raft state; fsync happens while holding it                                                                                                                                                       | Easiest to reason about; the reply never leaves before the data is on disk                                                           | Disk latency blocks other handlers. Stage 4 measured the effect: durable writes top out around 230/s (see Measured numbers) |
| Append-only WAL with CRC frames: hard-state, entry, truncate records                                                                                                                                                  | Cheap appends; a half-written final record after a crash is detected and dropped                                                     | File only grows (no log compaction); mid-file corruption is a hard error rather than silent data loss                       |
| Leader appends a no-op entry on election                                                                                                                                                                              | Lets entries from earlier terms commit (Figure 8 rule) and gives Stage 3 reads a current-term anchor                                 | One extra ledger entry per election                                                                                         |
| Commit only counts replicas of CURRENT-term entries                                                                                                                                                                   | Figure 8: old-term entries can be overwritten even when a majority holds them                                                        | Old entries commit indirectly via the no-op                                                                                 |
| Follower replies with ConflictIndex so the leader skips back quickly                                                                                                                                                  | Avoids stepping back one entry per round trip                                                                                        | Slightly more logic than the paper's basic version                                                                          |
| Randomized election timeout 300-600ms, heartbeat 75ms (defaults)                                                                                                                                                      | Avoids split votes; heartbeat much shorter than timeout                                                                              | Failover takes roughly one timeout                                                                                          |
| Persist errors panic                                                                                                                                                                                                  | A node that cannot save its promises must not keep running                                                                           | A full disk crashes the node                                                                                                |
| Apply callback runs on one goroutine, in order, outside the lock                                                                                                                                                      | State machine sees every committed entry exactly once, in order                                                                      | Slow Apply delays later entries                                                                                             |
| Leader steps down if it has not heard from a majority within one minimum election timeout (check-quorum)                                                                                                              | A cut-off leader stops accepting writes and stops claiming leadership at about the time a replacement can be elected                 | A leader can step down needlessly if replies are delayed or lost for a whole timeout; the cost is just a new election       |
| Reads use read-index (ConfirmRead): wait for a commit in the leader's own term, note the commit index, get a majority to answer a heartbeat sent AFTER the read arrived, wait for the Store to apply up to that index | Linearizable reads without writing every read into the log; does not depend on clocks                                                | Each read costs a round trip to a majority; a leader that cannot reach a majority refuses (503) instead of answering        |
| Only replies to heartbeats built after the read count as confirmation                                                                                                                                                 | A reply to an older heartbeat only proves we led at some earlier moment                                                              | Slightly more bookkeeping (a counter on each heartbeat)                                                                     |
| Request IDs = Client-Id + a per-client counter (Request-Id 1, 2, 3...); the cluster remembers each client's last request number and its answer                                                                        | Bounded memory, simple to explain, and a retry is recognised even by a new leader because the memory is part of the replicated state | One outstanding request per client ID; the table grows with the number of clients and never expires                         |
| Duplicates are filtered at APPLY time, not when the request arrives                                                                                                                                                   | Apply is the one place every node sees the same requests in the same order                                                           | A retry still takes a (cheap) slot in the ledger                                                                            |
| Request IDs are optional                                                                                                                                                                                              | Easy to use with plain curl                                                                                                          | Without them a retried write can be applied twice (a test shows this on purpose)                                            |

## Fault-injection testing (Stage 4)

`kv/faults_test.go` runs a REAL 3-node cluster (real sockets, real disk files) while clients keep working and faults are injected. `kv/history_test.go` is the referee that checks every answer afterwards.

**Workload.** Each of 3 keys has one writer that writes 1, 2, 3, ... in order, retrying each write with the same Request-Id until acknowledged. 4 readers read random keys from random nodes (refusals are skipped; wrong answers are not allowed). A "duplicator" re-sends old, already-acknowledged writes late, like a delayed network message.

**Referee rules** (possible because each key has a single counting writer):

- too old: a write acknowledged before a read began must be visible to it;
- from the future: a value whose write had not started before the read ended is impossible;
- going backwards: if one read finished before another began, the later one cannot see an older value.
  After healing, all three nodes must hold every key's last acknowledged value (nothing acknowledged was lost, replicas agree). The referee itself is tested against hand-made good and bad histories.

**Faults.** Kill the leader and restart it; cut the leader off from the other nodes (clients can still reach it); the same partition with step-down disabled so the leader never notices (`raft.Config.DisableStepDown`, tests only); restart nodes one at a time then crash all three at once; seeded random mix incl. losing a majority (`CHAOS_SEED=<n>` replays the fault choices).

**What each layer catches (found by breaking the code on purpose).** The fault suite catches: naive or majority-less reads (as stale reads seen by clients), dedupe switched off, committing without a majority, log entries not saved to disk. It does NOT reliably catch a vote that is not saved or a stale node winning an election; those need one precise sequence of events, so targeted raft-level tests catch them (`TestVoteSurvivesRestart`, `TestStaleNodeCannotBecomeLeader`, `TestLeaderOwnCopyCountsSoItMustBeOnDisk`). Broad fault tests and narrow targeted tests are both needed.

**Lesson learned.** The first version of the workload used a 3 s client timeout. Requests stuck on a cut-off leader then blocked every writer for the whole partition, so no write was acknowledged while the old leader served reads and a stale-read bug went unnoticed. Client timeout is now 600 ms so stuck requests retry elsewhere.

## Docker (Stage 5)

### The ideas, in plain language

- **Image**: a frozen, sealed snapshot of a program plus the files it needs, read-only. Like a recipe with the ingredients photographed so anyone can make the same dish. Ours is `raft-kv:dev`.
- **Layers**: an image is a stack of transparent sheets. Each line of the Dockerfile adds a sheet. If nothing that went into a sheet has changed, Docker reuses it instead of rebuilding, so the things that change least go first.
- **Container**: a running copy made from an image, like a cake baked from the recipe. It gets its own thin scratch layer to write on. Delete the container and the scratch layer goes with it.
- **Volume**: a separate drawer that lives outside any container. Plugging a volume in at `/data` means everything written to `/data` goes into the drawer. Delete the container and the drawer, with the notebook inside, stays. A new container plugged into the same drawer finds the same notebook.
- **Multi-stage build**: stage 1 is a big kitchen (the whole Go toolchain) that cooks the program; stage 2 is an empty plate that receives only the finished program. The final image is the program plus nothing else (about 5.6 MB of program in an image with no shell, no tools, no operating system files).
- **Why the volume, not the container, survives a crash**: each node writes its term, its vote and its ledger to `/data` before replying to anyone. `/data` is a volume. The container's own layer is disposable by design. So the ledger outlives any container, whether it was killed, stopped or deleted.

### Files

`Dockerfile` (build), `.dockerignore` (keeps the build small), `docker-compose.yml` (3 services `kv0`..`kv2`, volumes `kv0-data`..`kv2-data`, ports 8081..8083 on the host), `cmd/kvproof` (the experiment).
Containers reach each other as `kv0:8080`, `kv1:8080`, `kv2:8080` on Docker's private network. Those names only exist there, so `-client-urls` tells followers to redirect outside clients to `localhost:808x` instead. No restart policy on purpose, so a killed container stays dead until you start it.

### How to run

    docker compose up -d --build            # start the cluster
    curl -s localhost:8081/raft/status      # who is leader? (8082, 8083 for the others)
    curl -L -X PUT localhost:8082/kv/alice -d 42
    curl -L localhost:8083/kv/alice
    docker compose down                     # remove containers, KEEP volumes
    docker compose down -v                  # remove containers AND volumes (erases the nodes' memory)
    go run ./cmd/kvproof                    # the experiment (starts from a clean slate; deletes the raftkv volumes first)

### What `kvproof` does and checks (13 checks)

1. Starts 3 containers; checks all reachable and a leader is elected.
2. Starts 4 writers; checks writes flow. A write only counts once the cluster acknowledges it; retries reuse the same request number.
3. `docker kill` (SIGKILL) the leader mid-traffic. The clock for "while it was down" starts only once the node is CONFIRMED down, because the kill command itself takes a moment and writes keep flowing until the process really dies.
4. Checks a new leader (higher term) was elected without it, and that at least 50 writes were acknowledged while it stayed down for at least 3 s.
5. `docker compose start` the same container. Checks that, on startup, the node **read back from its own volume** at least the ledger entries it had before the kill, and its term (`recoveredEntries` / `recoveredTerm` in `/raft/status`); that the cluster had moved AHEAD of what the volume held (so catching up is real work); and that it caught up.
6. `docker compose rm -sf` deletes the container, then `up` creates a brand-new one on the same volume. Checks it reads back the ledger, including the entries it received AFTER the first restart, and catches up again.
7. Stops writing; checks all three nodes end with identical, fully applied ledgers, and that every acknowledged write is still in the cluster.
   It also prints what it measured: total writes acknowledged, how many while the node was down, and how long until a new leader was found. If anything fails, the cluster is left running and the recent logs are printed.

### What has and has not been verified

Verified by the author's first real Docker run (2026-11-10, Docker Desktop on an Apple M3): the image builds (the `golang:1.27` tag exists), the volumes are created and writable by user 65532, the nodes find each other by service name, and `docker kill`, `start`, `rm -sf` and `up` behave as the proof expects. All 11 checks of the first version of the proof passed.

What that first run genuinely proved: after a SIGKILL, `kv2` read 211 ledger entries from its own volume (a wiped volume reads 0); a brand-new container created after `docker compose rm` read the same 211; at the end all three nodes were identical and all 210 acknowledged writes were present.

What the first run did NOT prove, found by reading its output (all fixed in the current `kvproof`):

- "92 more writes acknowledged while the node was down" counted writes made before the node actually died (the node had 118 entries when its status was read but 211 on disk at death, so about 93 writes landed during the kill command itself).
- "Caught up (leader had committed 0 entries)": no leader was found at that instant, so the check compared against zero and passed automatically.
- Step 6 began before the node had caught up, and the whole experiment took about 2 seconds, too short to show the cluster working while the node was down.
  The same weaknesses were reproduced in the stand-in first: killing a second node (no majority, so no writes possible) still made the old proof print "PASS writes were still acknowledged"; the current proof fails that case at step 4, fails a wiped volume at step 5, and passes the healthy case on every run.

Verified without Docker (sandbox): the server compiles from exactly the files the Dockerfile copies, as a static 5.6 MB program, and runs in an EMPTY root filesystem as user 65532: serves requests, writes its WAL, shuts down cleanly on SIGTERM, recovers its data after SIGKILL on the same folder, and gives a clear `permission denied` if the data folder has the wrong owner. `docker-compose.yml` is internally consistent. `kvproof`'s logic was tested against a stand-in `docker` command that runs real server processes on real directories.

### If something goes wrong

- Build says the Go toolchain is too old or the image is not found: change `ARG GO_VERSION` at the top of the Dockerfile to match `go version` on your machine.
- `docker compose logs kv0` shows `permission denied` on `/data/raft.wal`: the volume is owned by root. Run `docker compose down -v` and try again (a new empty volume copies ownership from the image). As a last resort add `user: "0"` under the services.
- Ports 8081 to 8083 already in use: stop the other program using them.
- A redirect fails: use `curl -L`, and check `-client-urls` in the compose file.

### What this does and does not prove

`docker kill` crashes the PROCESS. It shows a crashed node recovers from its volume and that deleting a container loses nothing. It does not show survival of a power cut or machine crash. On Docker Desktop the volume lives on a disk file inside a Linux VM, so whether an fsync reaches the Mac's physical disk before a power cut depends on Docker Desktop and is untested here. A node whose volume is DELETED comes back blank with the same identity; Raft does not make that safe (it may have forgotten votes and entries it already acknowledged), so it must be treated as a dead member. Stage 6 covers this.

### Proof run on the author's machine

Docker Desktop on an Apple M3, 2026-11-10, current `kvproof` (13 checks): **13 passed, 0 failed.** Killed node: `kv1`, which was the leader. One run.

| What                                                               | Measured                                                                                              |
| ------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| New leader after the kill                                          | node 0, term 2, found 272 ms after `kv1` was confirmed down                                           |
| Writes acknowledged while `kv1` was down (3 s)                     | 519                                                                                                   |
| Writes acknowledged in total                                       | 969 (all present at the end, 0 missing, 0 wrong)                                                      |
| `kv1`'s ledger just before the kill (last status read)             | 61 entries                                                                                            |
| What `kv1` read from its own volume on restart                     | 147 entries, term 1 (more than 61 because writes landed during the kill command itself)               |
| Cluster's committed entries when `kv1` restarted                   | 714                                                                                                   |
| `kv1` caught up                                                    | in 60 ms, ledger 147 to 796                                                                           |
| Brand-new container after `docker compose rm` read from the volume | 877 entries, term 2 (the first restart had read 147, so the volume also held what was saved after it) |
| End state                                                          | all three nodes identical and fully applied                                                           |

Notes on reading these: the 272 ms is measured from the moment the node was confirmed down, by polling every 50 ms, so it is approximate; it is consistent with the configured election timeout of 300 to 600 ms, which starts counting from the last heartbeat. It is a single sample, not an average. Throughput during this run is not a benchmark: the writers are sequential clients and the proof also polls and runs Docker commands.

What this does NOT show: survival of a power cut or machine crash (`docker kill` crashes a process; on Docker Desktop the volume sits on a disk file inside a Linux VM, and whether fsync reaches the Mac's physical disk is untested); behaviour with a deleted volume (that node would come back blank, which Raft does not make safe; Stage 6); more than one run.

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

- `go vet ./...` clean; `go test -race ./...`: 73 tests pass (kv: 34, raft: 39). Includes a 5-node test (survives 2 failures, refuses to commit with 3 down).
- Repeated 8x in full and the chaos test 10x with random fault schedules: no failures, no data races.
- Mutation check: each of these deliberate bugs makes at least one test fail: no election restriction, committing old-term entries by counting, committing without a majority, not persisting the vote, not persisting the log, accepting a stale leader's messages, truncating the log on a duplicate message.
- Stage 3 step-down: new tests (cut-off leader steps down and refuses writes; only one leader claimed after a partition; leader with a majority keeps leading) pass. Breaking the check on purpose (never step down; step down unless hearing everyone) makes them fail. Full suite repeated 5x: no failures.
- Stage 3 reads: a test reproduces the bug on purpose (a cut-off leader answering from its own data returns balance=100 after balance=150 was committed) and the safe read refuses instead. A churn test (3 readers, a writer, the leader cut off mid-run) saw tens of thousands of confirmed reads and no stale answer. White-box tests control which heartbeat reply arrives when. Breaking each step of the read recipe on purpose (skip the current-term commit wait; skip the majority check; accept replies to older heartbeats; skip waiting for the Store; skip the leader check) makes a test fail every time.
- Stage 3 retries: a retried write no longer overwrites a newer value written by someone else; a retried delete repeats its original answer; this holds after the leader is killed and after a full restart. Breaking the dedupe three ways makes tests fail.
- Stage 4 fault injection (live traffic): each scenario handled 500 to 1500 acknowledged writes and 1500 to 3000 checked reads, no wrong answers, all replicas identical after healing (random chaos with a majority lost: far fewer writes got through, as expected). Broken-on-purpose versions caught: naive read, read without majority check, dedupe off, commit without a majority, log not saved to disk, leader's own entry not saved to disk (the last via a new deterministic test).
- Full suite repeated 3x, plus the chaos and partition scenarios 4x more: no failures, no data races.
- Manual (3 processes): A's request 1, B's write, A's retry of request 1 left the value as B's; the same repeat without IDs overwrote it. A read after `kill -9` of the leader worked through the new leader.
- Manual (Stage 2): 3 separate processes with real fsync. Wrote through a follower, `kill -9` the leader, wrote and read during the outage, restarted the leader from disk; it rejoined as follower with commitIndex = lastApplied = 4.

## Measured numbers

**Rule: only numbers that `scripts/bench.sh` printed on the author's own machine go in this section.** Nothing estimated, nothing from another machine, nothing rounded up. Every table says which machine, which date, and which commit.

### How to measure

    bash scripts/bench.sh 3 durable      # 3 nodes, real fsync: the headline numbers
    bash scripts/bench.sh 5 durable      # 5 nodes
    bash scripts/bench.sh 3 nofsync      # same, with fsync OFF: shows what durability costs; never a headline number

All nodes run on one machine (so no real network latency). Each run uses 100-byte values, writes carry request IDs (the exactly-once path), 10 s measured after a 2 s warm-up, at 1, 8, 32 and 128 concurrent clients. Reads are the safe kind (every read confirms leadership with a majority first). After every write run, kvbench reads back what it wrote and refuses to call the run valid if any value is wrong; it also flags a run during which the leader changed. Raw output is saved under `bench-results/`.

Close other heavy programs and plug the laptop in before measuring.

### Results (author's machine)

Machine: Apple M3 (8 cores), macOS 26.6.2, Go 1.27.3, darwin/arm64. All nodes and the load generator on this one laptop.
Date: 2026-11-10. Commit: _(paste `git rev-parse --short HEAD`)_.
Every run: 0 errors, all keys read back correctly, no leader change during the run. One 10 s run per cell, so small differences are not meaningful (point 5 below shows noise of at least 12% between two runs that should match).
Raw output: `bench-results/20261110-003538-3node-durable.txt`, `...-004525-5node-durable.txt`, `...-005025-3node-nofsync.txt`.

**3 nodes, durable (real fsync): the headline numbers**

| workload        | clients | ops/sec | p50 ms | p95 ms | p99 ms  | max ms  |
| --------------- | ------- | ------- | ------ | ------ | ------- | ------- |
| write           | 1       | 119     | 8.95   | 10.24  | 12.89   | 33.16   |
| write           | 8       | 208     | 34.37  | 60.90  | 64.96   | 72.12   |
| write           | 32      | 233     | 127.60 | 246.32 | 264.49  | 306.77  |
| write           | 128     | 229     | 548.80 | 831.58 | 1047.29 | 1133.93 |
| read            | 1       | 6774    | 0.14   | 0.16   | 0.20    | 0.44    |
| read            | 8       | 23523   | 0.33   | 0.44   | 0.52    | 1.24    |
| read            | 32      | 36079   | 0.88   | 1.12   | 1.41    | 3.43    |
| read            | 128     | 40673   | 3.14   | 3.77   | 4.27    | 6.95    |
| mixed 90% reads | 1       | 1024    | 0.37   | 6.41   | 8.46    | 15.80   |
| mixed 90% reads | 8       | 1433    | 4.42   | 14.05  | 17.31   | 28.82   |
| mixed 90% reads | 32      | 1685    | 17.45  | 34.09  | 54.46   | 190.84  |
| mixed 90% reads | 128     | 2534    | 43.73  | 94.85  | 122.50  | 200.24  |

**5 nodes, durable**

| workload        | clients | ops/sec | p50 ms | p95 ms | p99 ms  | max ms  |
| --------------- | ------- | ------- | ------ | ------ | ------- | ------- |
| write           | 1       | 98      | 10.08  | 13.93  | 16.25   | 20.01   |
| write           | 8       | 188     | 38.19  | 68.53  | 73.58   | 78.49   |
| write           | 32      | 208     | 142.62 | 270.29 | 300.47  | 317.51  |
| write           | 128     | 215     | 585.00 | 776.93 | 1129.41 | 1210.76 |
| read            | 1       | 5157    | 0.19   | 0.22   | 0.28    | 13.72   |
| read            | 8       | 17661   | 0.45   | 0.57   | 0.69    | 5.32    |
| read            | 32      | 31054   | 1.02   | 1.28   | 1.61    | 4.01    |
| read            | 128     | 39360   | 3.25   | 3.85   | 4.31    | 6.27    |
| mixed 90% reads | 1       | 800     | 0.47   | 8.24   | 11.37   | 16.57   |
| mixed 90% reads | 8       | 1152    | 5.93   | 17.10  | 21.17   | 29.70   |
| mixed 90% reads | 32      | 1629    | 19.36  | 32.16  | 40.94   | 65.93   |
| mixed 90% reads | 128     | 2407    | 45.21  | 102.62 | 133.42  | 240.48  |

**3 nodes, fsync OFF (shows the cost of durability; unsafe, never a headline)**

| workload        | clients | ops/sec | p50 ms | p95 ms | p99 ms | max ms |
| --------------- | ------- | ------- | ------ | ------ | ------ | ------ |
| write           | 1       | 6182    | 0.16   | 0.18   | 0.24   | 3.22   |
| write           | 8       | 21435   | 0.37   | 0.48   | 0.56   | 8.18   |
| write           | 32      | 32932   | 0.95   | 1.24   | 1.63   | 10.41  |
| write           | 128     | 36576   | 3.45   | 4.16   | 5.30   | 36.01  |
| read            | 1       | 6735    | 0.15   | 0.16   | 0.18   | 0.68   |
| read            | 8       | 21721   | 0.36   | 0.47   | 0.55   | 15.31  |
| read            | 32      | 32945   | 0.96   | 1.21   | 1.47   | 3.41   |
| read            | 128     | 36424   | 3.51   | 4.16   | 4.55   | 11.39  |
| mixed 90% reads | 1       | 6329    | 0.15   | 0.17   | 0.19   | 2.31   |
| mixed 90% reads | 8       | 20272   | 0.39   | 0.50   | 0.59   | 3.57   |
| mixed 90% reads | 32      | 31756   | 1.00   | 1.27   | 1.59   | 5.05   |
| mixed 90% reads | 128     | 35232   | 3.62   | 4.33   | 4.74   | 9.96   |

### What the numbers say

Ratios below are arithmetic on the tables above. Explanations marked "consistent with" are inferences, not measurements.

1. **Durable writes are limited to about 230 per second.** Throughput goes 119, 208, 233, 229 as clients go 1, 8, 32, 128: it stops growing at about 8 clients while median latency keeps climbing (34, 128, 549 ms). That is a queue behind one bottleneck. Check (clients should be about throughput x latency): 233 ops/s x 0.1276 s = 29.7 for 32 clients, and 229 ops/s x 0.5488 s = 125.7 for 128 clients, so the figures are self-consistent.
2. **fsync is the cost.** With fsync off, writes reach 6182/s at 1 client and 36576/s at 128 clients, versus 119 and 229 durable: about 52x and 160x. Median single-client write latency is 8.95 ms durable versus 0.16 ms.
3. **fsync on this machine, measured directly: 2.52 ms** per 100-byte append plus fsync (average of 200, one run, a separate small Go program, not part of this repo). A single write has two flushes in a row (the leader's, then a follower's), about 5.05 ms of the 8.95 ms median (56%). The other roughly 3.9 ms is NOT explained by fsync and has not been measured (candidates: HTTP and JSON handling, goroutine scheduling, waiting for the node's lock).
   The ceiling is also not explained by fsync alone: 233 writes/s is one write per 4.3 ms, but one 2.52 ms flush per write would allow about 396 writes/s, so the ceiling is 59% of that. Something else that runs one at a time costs about 1.8 ms per write. A guess, not a finding: the leader holds its lock while flushing, and the goroutines that send entries and handle replies need the same lock.
4. **Reads are far cheaper than writes.** At 1 client: 0.14 ms median versus 8.95 ms (about 64x). At 128 clients: 40673 reads/s versus 229 writes/s. Reads do no disk write, only a round trip to a majority.
5. **Read speed is the same with and without fsync** (40673 vs 36424 at 128 clients). The durable run was actually 12% HIGHER, which fsync cannot cause, so that gap shows run-to-run noise of at least that size. That is what you expect when reads never write to disk.
6. **A 10% write mix is capped by write speed.** At 128 clients the mix did 2534 ops/s; 10% of that is about 253 writes/s, close to the pure-write ceiling of 229. Even a small share of writes pulls total throughput from about 40000 down to about 2500.
7. **5 nodes were slower than 3 in all 12 durable cells** (4 client levels x write, read, mixed): writes by 6% to 18% (98 vs 119, 188 vs 208, 208 vs 233, 215 vs 229 ops/s at 1, 8, 32, 128 clients), reads by 3% to 25%, mixed by 3% to 22%. The direction is consistent; the sizes are uncertain with one run each, and point 5 shows noise of 12% or more is possible. Plausible causes (not measured): more followers to wait for and more messages competing for the same CPU and disk.
8. **Tail latency for durable writes**: p99 is 1.4x the median at 1 client and about 1.9x to 2.1x at 8 or more clients (for example 264 ms vs 128 ms at 32 clients). The slowest request was about 2.1x the median at 128 clients.

### Predictions versus results

- Writes limited by disk flushes, plateauing at modest concurrency: **partly confirmed**. The plateau and the 52x to 160x cost of fsync are confirmed (points 1 and 2); but my explanation of the exact figures was wrong: I predicted 4 to 5 ms per flush, the measured flush is 2.52 ms, so fsync accounts for only part of the write latency and ceiling (point 3).
- Reads faster than writes: **confirmed**, by far more than expected (point 4).
- 5 nodes write a little slower than 3: **consistent**, 6% to 18% lower (point 7).

### Known limits of these measurements

All nodes share one machine, one disk and one CPU, so they compete with each other and with the load generator. There is no real network between nodes. One 10 s run per cell, no repeats, so small differences (under about 10%) may be noise. Numbers describe this laptop, not a production cluster. fsync time was measured once with a separate 200-iteration program (2.52 ms), not inside the server. Stage 6 repeats the runs on Kubernetes.

### Ideas the numbers point to (not done)

Group commit: save several waiting writes to disk with one fsync instead of one fsync per write. It should help, but point 3 shows fsync does not explain the whole ceiling, so the gain is unknown. Profile first (Go's pprof) to see where the time goes, and cut lock hold times if the lock is the problem. Any code change means repeating the runs.
