#!/usr/bin/env bash
# Remove the footprint this scenario created from the shared cluster.
set -euo pipefail
MODE="${1:?usage: cleanup.sh <cli|k8s>}"
case "$MODE" in
  k8s) kubectl delete kafkatopic observe-demo --ignore-not-found ;;
  cli)
    # The CLI has no topic-delete command (apply never deletes a topic absent
    # from the desired set, and --prune is ACL-scope only), so there is nothing
    # to do here. CLI-mode isolation instead relies on the orchestrator standing
    # up a fresh broker per suite (compose up/down -v) plus per-scenario unique
    # topic names. No-op by design.
    : ;;
  *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac
