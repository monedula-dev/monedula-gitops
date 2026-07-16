#!/usr/bin/env bash
# Remove the footprint this scenario created from the shared cluster.
set -euo pipefail
MODE="${1:?usage: cleanup.sh <cli|k8s>}"
case "$MODE" in
  k8s) kubectl delete kafkatopic schema-demo --ignore-not-found ;;
  cli)
    # The CLI has no topic-delete command; CLI-mode isolation relies on a fresh
    # broker per suite (compose up/down -v) plus per-scenario unique topic
    # names. No-op by design (see 01-create-topic).
    : ;;
  *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac
