#!/usr/bin/env bash
# Remove the role binding this scenario created.
set -euo pipefail
MODE="${1:?usage: cleanup.sh <cli|k8s>}"
case "$MODE" in
  k8s) kubectl delete kafkarolebinding checkout-read --ignore-not-found ;;
  cli)
    # No CLI role-binding delete command exists; CLI-mode isolation relies on a
    # fresh broker per suite + unique resource names. No-op by design.
    : ;;
  *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac
