#!/usr/bin/env bash
# Measure the cluster on THIS machine.
#
#   bash scripts/bench.sh [NODES] [durable|nofsync] [DURATION]
#
#   NODES     3 (default) or 5
#   durable   real fsync before every reply: the honest configuration (default)
#   nofsync   skips fsync; shows what durability costs. NOT safe, never a headline number.
#   DURATION  measured seconds per run (default 10s)
#
# Results are printed, saved to bench-results/, and appended to bench-results/results.csv.
set -euo pipefail

NODES="${1:-3}"
MODE="${2:-durable}"
DURATION="${3:-10s}"
BASE_PORT=9001

case "$MODE" in
  durable) FSYNC_FLAG="" ;;
  nofsync) FSYNC_FLAG="-unsafe-no-fsync" ;;
  *) echo "mode must be durable or nofsync"; exit 2 ;;
esac

cd "$(dirname "$0")/.."
WORK="$(mktemp -d)"
mkdir -p bench-results
OUT="bench-results/$(date +%Y%m%d-%H%M%S)-${NODES}node-${MODE}.txt"

PIDS=""
cleanup() {
  for p in $PIDS; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

echo "building..."
go build -o "$WORK/kvserver" ./cmd/kvserver
go build -o "$WORK/kvbench" ./cmd/kvbench

PEERS=""
for i in $(seq 0 $((NODES - 1))); do
  url="http://127.0.0.1:$((BASE_PORT + i))"
  PEERS="${PEERS:+$PEERS,}$url"
done

for i in $(seq 0 $((NODES - 1))); do
  "$WORK/kvserver" -id "$i" -peers "$PEERS" -data "$WORK/node$i" $FSYNC_FLAG >"$WORK/node$i.log" 2>&1 &
  PIDS="$PIDS $!"
done

{
  echo "== machine =="
  uname -sm
  if [ "$(uname -s)" = "Darwin" ]; then
    echo "cpu:    $(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)"
    echo "cores:  $(sysctl -n hw.ncpu 2>/dev/null || true)   macOS $(sw_vers -productVersion 2>/dev/null || true)"
  else
    echo "cores:  $(nproc 2>/dev/null || true)"
  fi
  echo "mode:   $MODE   nodes: $NODES   (all nodes run on this one machine)"
  echo
  "$WORK/kvbench" -nodes "$PEERS" -duration "$DURATION" -concurrency "1,8,32,128" \
    -workloads "write,read,mixed" -label "${NODES}-node-${MODE}" -out bench-results/results.csv
} 2>&1 | tee "$OUT"

echo
echo "saved: $OUT"