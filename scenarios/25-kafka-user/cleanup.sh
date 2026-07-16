#!/usr/bin/env bash
# Remove the footprint this scenario created from the shared cluster.
set -euo pipefail
MODE="${1:?usage: cleanup.sh <cli|k8s>}"
case "$MODE" in
  k8s)
    kubectl delete kafkatopic users-orders --ignore-not-found
    kubectl delete kafkauser svc-orders-app --ignore-not-found
    ;;
  cli)
    # No CLI command deletes a topic or a SCRAM credential (apply never
    # deletes a topic/user absent from the desired set), so there is nothing
    # to do here. CLI-mode isolation instead relies on the orchestrator
    # standing up a fresh broker per suite (compose up/down -v) plus
    # per-scenario unique resource names. No-op by design (see
    # 05-topic-with-access, 07-user-quota).
    : ;;
  *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac
