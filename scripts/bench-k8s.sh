#!/usr/bin/env bash
# Measure the cluster running on Kubernetes (kind) on THIS machine, with the same
# tool and the same settings as scripts/bench.sh, so the numbers are comparable.
#
#   bash scripts/bench-k8s.sh [DURATION]        (default 10s per run)
#
# Start the cluster first:  bash scripts/k8s.sh up
# The load goes to the leader pod's own door (localhost:30081..30083), exactly
# as bench.sh sends it straight to the leader process.
set -euo pipefail

DURATION="${1:-10s}"
NODES="http://localhost:30081,http://localhost:30082,http://localhost:30083"

cd "$(dirname "$0")/.."
curl -sf --max-time 3 http://localhost:30081/raft/status >/dev/null || {
  echo "the cluster is not reachable on localhost:30081. Start it with: bash scripts/k8s.sh up"
  exit 1
}

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p bench-results
OUT="bench-results/$(date +%Y%m%d-%H%M%S)-3node-k8s-durable.txt"
go build -o "$WORK/kvbench" ./cmd/kvbench

{
  echo "== machine =="
  uname -sm
  if [ "$(uname -s)" = "Darwin" ]; then
    echo "cpu:    $(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)"
    echo "cores:  $(sysctl -n hw.ncpu 2>/dev/null || true)   macOS $(sw_vers -productVersion 2>/dev/null || true)"
  fi
  echo "mode:   kubernetes (kind, ONE node), 3 pods, real fsync, a PersistentVolumeClaim each"
  echo "path:   this program -> Docker port mapping -> kube-proxy -> leader pod"
  echo
  "$WORK/kvbench" -nodes "$NODES" -duration "$DURATION" -concurrency "1,8,32,128" \
    -workloads "write,read,mixed" -label "3-node-k8s-durable" -out bench-results/results.csv
} 2>&1 | tee "$OUT"

echo
echo "saved: $OUT"