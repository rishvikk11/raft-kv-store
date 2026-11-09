# Fault-Tolerant Replicated KV Store (Go)

A key-value store replicated across 3 nodes with Raft, later run in Docker and local Kubernetes.
Implemented from the Raft paper (raft.github.io/raft.pdf) and public Go/Docker/Kubernetes docs only.

## Stages

- [ ] Stage 1: single-server store (in progress: in-memory store done, network interface next)
- [ ] Stage 2: Raft consensus
- [ ] Stage 3: linearizable reads, request IDs, leader step-down
- [ ] Stage 4: fault-injection tests and measurements
- [ ] Stage 5: Docker / Compose
- [ ] Stage 6: local Kubernetes

## Layout

    kv/store.go        the state machine: map + RWMutex (Get, Put, Delete)
    kv/store_test.go   unit and concurrency tests
    (next) cmd/kvserver/main.go   HTTP interface over the store

## Architecture

The `kv.Store` knows nothing about networks, disks or Raft. Later stages wrap it:
clients -> HTTP server -> Raft log -> (once committed) Store.

## Design decisions

| Decision                             | Why                                                             | Tradeoff                                                                                        |
| ------------------------------------ | --------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `sync.RWMutex` around one map        | Simple, obviously correct, many concurrent readers              | One lock for the whole map; fine at this scale, sharding is an option if measurements demand it |
| `Get` returns `(value, ok)`          | An empty value is a real value; missing must be distinguishable | Callers must check `ok`                                                                         |
| `Delete` returns whether key existed | Lets the server answer 404 vs 204 honestly                      | None meaningful                                                                                 |
| Strings for keys and values          | Simplest thing that works                                       | No binary-safe values yet                                                                       |

## How to run the tests

    go vet ./...
    go test -race -count=1 -v ./...

## Current test results

- 2026-11-07: `go test -race ./...` passes (7 tests, Go 1.22.2). Sanity check: with write locks removed, the race detector reports a DATA RACE in `TestConcurrentAccess`, so the test does catch locking bugs.

## Measured numbers

None yet. Only numbers we actually measure get recorded here.
