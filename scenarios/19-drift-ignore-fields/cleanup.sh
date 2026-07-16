#!/usr/bin/env bash
# Remove the footprint this scenario created from the shared cluster.
set -euo pipefail
MODE="${1:?usage: cleanup.sh <cli|k8s>}"
case "$MODE" in
  k8s) kubectl delete kafkatopic ignore-demo --ignore-not-found ;;
  cli)
    # The CLI has no topic-delete command, so there is nothing to do here.
    # CLI-mode isolation relies on the orchestrator standing up a fresh broker
    # per suite (compose up/down -v) plus per-scenario unique names. No-op.
    : ;;
  *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac
