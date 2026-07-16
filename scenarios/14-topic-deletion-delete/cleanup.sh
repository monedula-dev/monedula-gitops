#!/usr/bin/env bash
# Remove the footprint this scenario created from the shared cluster.
set -euo pipefail
MODE="${1:?usage: cleanup.sh <cli|k8s>}"
case "$MODE" in
  k8s) kubectl delete kafkatopic delete-demo --ignore-not-found ;;
  cli)
    # k8s-only scenario (deletion is operator/finalizer-driven; the CLI has no
    # topic-delete). Nothing to do in cli mode. No-op by design.
    : ;;
  *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac
