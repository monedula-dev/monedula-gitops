#!/usr/bin/env bash
# Remove the footprint this scenario created.
set -euo pipefail
MODE="${1:?usage: cleanup.sh <cli|k8s>}"
case "$MODE" in
  cli)
    # Mirrors are never deleted by the CLI; the runner tears the whole
    # cluster-link stack down (compose down -v) after the suite. No-op.
    : ;;
  k8s)
    # cli-only scenario; nothing to clean up.
    : ;;
  *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac
