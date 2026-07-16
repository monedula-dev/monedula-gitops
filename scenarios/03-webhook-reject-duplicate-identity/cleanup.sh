#!/usr/bin/env bash
# Remove the first topic (a) that was successfully created in this scenario.
# The second topic (b) was rejected by the webhook and was never persisted.
set -euo pipefail
MODE="${1:?usage: cleanup.sh <cli|k8s>}"
case "$MODE" in
  k8s) kubectl delete kafkatopic dup-orders-a dup-orders-b --ignore-not-found ;;
  *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac
