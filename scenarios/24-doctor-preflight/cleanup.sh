#!/usr/bin/env bash
# doctor is read-only — it creates nothing, so cleanup is a no-op in both modes.
set -euo pipefail
MODE="${1:?usage: cleanup.sh <cli|k8s>}"
case "$MODE" in
  cli|k8s) : ;;
  *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac
