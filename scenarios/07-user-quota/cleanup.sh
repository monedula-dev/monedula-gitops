#!/usr/bin/env bash
# Remove the footprint this scenario created from the shared cluster.
set -euo pipefail
MODE="${1:?usage: cleanup.sh <cli|k8s>}"
case "$MODE" in
  k8s) kubectl delete kafkaquota svc-quota --ignore-not-found ;;
  cli)
    # No CLI quota-delete command exists; CLI-mode isolation relies on a fresh
    # broker per suite (compose up/down -v) plus per-scenario unique resource
    # names. No-op by design (see 01-create-topic).
    : ;;
  *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac
