#!/usr/bin/env bash
# Remove the topic this scenario created from the auth-mtls cluster.
set -euo pipefail
MODE="${1:?usage: cleanup.sh <cli|k8s>}"
case "$MODE" in
  k8s) kubectl delete kafkatopic mtls-demo --ignore-not-found ;;
  cli)
    # No CLI topic-delete command exists; CLI-mode isolation relies on a fresh
    # broker per suite + unique topic names. No-op by design (see 01-create-topic).
    : ;;
  *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac
