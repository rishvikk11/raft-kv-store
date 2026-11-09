# Fault-Tolerant Replicated KV Store (Go)

A key-value store replicated across 3 nodes with Raft, later run in Docker and local Kubernetes.
Implemented from the Raft paper (raft.github.io/raft.pdf) and public Go/Docker/Kubernetes docs only.

## Stages

- [ ] Stage 1: single-server store (in-memory store + HTTP interface, race-clean)
- [ ] Stage 2: Raft consensus
- [ ] Stage 3: linearizable reads, request IDs, leader step-down
- [ ] Stage 4: fault-injection tests and measurements
- [ ] Stage 5: Docker / Compose
- [ ] Stage 6: local Kubernetes

## Layout

    kv/store.go        the state machine: map + RWMutex (Get, Put, Delete)
    kv/store_test.go   unit and concurrency tests
    kv/server.go       HTTP handler: PUT/GET/DELETE /kv/{key}
    kv/server_test.go  HTTP tests (httptest, real concurrent clients)
    cmd/kvserver/main.go   runs the server (-addr flag, default :8080)

## Architecture

The `kv.Store` knows nothing about networks, disks or Raft. Later stages wrap it:
clients -> HTTP server -> Raft log -> (once committed) Store.

## Design decisions

| Decision                               | Why                                                                                 | Tradeoff                                                                                        |
| -------------------------------------- | ----------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `sync.RWMutex` around one map          | Simple, obviously correct, many concurrent readers                                  | One lock for the whole map; fine at this scale, sharding is an option if measurements demand it |
| `Get` returns `(value, ok)`            | An empty value is a real value; missing must be distinguishable                     | Callers must check `ok`                                                                         |
| `Delete` returns whether key existed   | Lets the server answer 404 vs 204 honestly                                          | None meaningful                                                                                 |
| HTTP, not raw TCP                      | Easy to poke with curl; Raft's server-to-server messages will be a separate concern | Slightly more overhead than a custom protocol                                                   |
| Value = raw request body, 1 MiB cap    | Simplest API; cap stops one client exhausting memory                                | Large values unsupported                                                                        |
| 204 on PUT/DELETE, 404 for missing key | Clients can tell "deleted" from "nothing there"                                     | None meaningful                                                                                 |
| Strings for keys and values            | Simplest thing that works                                                           | No binary-safe values yet                                                                       |

## How to run it

    go run ./cmd/kvserver -addr :8080
    curl -X PUT localhost:8080/kv/alice -d 42
    curl localhost:8080/kv/alice
    curl -X DELETE localhost:8080/kv/alice

## How to run the tests

    go vet ./...
    go test -race -count=1 -v ./...

## Current test results

- 2026-11-08: `go test -race ./...` passes (14 tests: 7 store + 7 HTTP, Go 1.22.2); manual curl smoke test of the built server passes (PUT 204, GET 200, DELETE 204, GET after delete 404).
- 2026-11-07: store tests pass (7 tests, Go 1.22.2). Sanity check: with write locks removed, the race detector reports a DATA RACE in `TestConcurrentAccess`, so the test does catch locking bugs.

## Measured numbers

None yet. Only numbers we actually measure get recorded here.
