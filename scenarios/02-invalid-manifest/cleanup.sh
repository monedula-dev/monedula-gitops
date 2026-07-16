#!/usr/bin/env bash
# Validation never touches the cluster, so there is nothing to clean up.
set -euo pipefail
MODE="${1:?usage: cleanup.sh <cli|k8s>}"
case "$MODE" in
  cli) exit 0 ;;
  *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac
