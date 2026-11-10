#!/usr/bin/env bash
# Run the Raft key-value store on a local Kubernetes cluster (kind).
#
#   bash scripts/k8s.sh up        build the image, create the cluster, deploy, wait until ready
#   bash scripts/k8s.sh status    show pods, disks, services and each node's Raft status
#   bash scripts/k8s.sh reload    rebuild the image and roll the pods onto it (after changing code)
#   bash scripts/k8s.sh down      delete the whole cluster (and every disk in it)
#
# Needs: Docker Desktop (running), kind, kubectl      brew install kind kubectl
set -euo pipefail

CLUSTER=raftkv
CTX="kind-$CLUSTER"
NS=raftkv
cd "$(dirname "$0")/.."

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing: $1   ($2)"; exit 1; }
}
need docker "install Docker Desktop and start it"
need kind "brew install kind"
need kubectl "brew install kubectl"
docker info >/dev/null 2>&1 || { echo "Docker is not running. Start Docker Desktop."; exit 1; }

kc() { kubectl --context "$CTX" -n "$NS" "$@"; }

# Build the image and put it INSIDE the kind cluster (kind cannot see your
# local Docker images by itself, and the pods are told not to pull from a registry).
build_and_load() {
  echo "building the image..."
  # No provenance/SBOM attachments: they turn the image into a "manifest list",
  # which kind sometimes fails to import.
  docker build --provenance=false --sbom=false -t raft-kv:dev .
  echo "loading the image into the cluster..."
  if ! kind load docker-image raft-kv:dev --name "$CLUSTER"; then
    echo "kind load docker-image failed; trying again through a tar file..."
    tmp="$(mktemp -d)"
    docker save raft-kv:dev -o "$tmp/raft-kv.tar"
    kind load image-archive "$tmp/raft-kv.tar" --name "$CLUSTER"
    rm -rf "$tmp"
  fi
}

case "${1:-}" in
  up)
    if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
      echo "cluster $CLUSTER already exists"
    else
      kind create cluster --name "$CLUSTER" --config deploy/k8s/kind.yaml --wait 180s
    fi
    build_and_load
    kubectl --context "$CTX" apply -f deploy/k8s/raftkv.yaml
    echo "waiting for the pods..."
    kc rollout status statefulset/kv --timeout=300s
    kc get pods,pvc,svc -o wide
    echo
    echo "ready. try:   curl -s localhost:30081/raft/status     and     bash scripts/k8s.sh status"
    ;;
  status)
    kc get pods,pvc,svc -o wide
    echo
    for p in 30081 30082 30083; do
      echo -n "localhost:$p  "
      curl -s --max-time 2 "localhost:$p/raft/status" || echo "(not reachable)"
      echo
    done
    ;;
  reload)
    build_and_load
    kc rollout restart statefulset/kv
    kc rollout status statefulset/kv --timeout=300s
    ;;
  down)
    kind delete cluster --name "$CLUSTER"
    ;;
  *)
    sed -n 2,9p "$0"
    exit 2
    ;;
esac