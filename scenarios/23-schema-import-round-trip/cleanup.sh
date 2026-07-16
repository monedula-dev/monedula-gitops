#!/usr/bin/env bash
# Remove state this scenario created. CLI-mode isolation relies on a fresh broker
# per suite + unique topic/subject names, so cli is a no-op (see 16-import-round-trip).
set -euo pipefail
MODE="${1:?usage: cleanup.sh <cli|k8s>}"
case "$MODE" in
  k8s) kubectl delete kafkatopic schema-import-demo --ignore-not-found ;;
  cli) : ;;
  *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac
