#!/usr/bin/env bash
# teardown.sh -- tear down the Monedula k8s e2e environment created by setup.sh:
# the host shared-sasl compose broker AND the monedula-e2e kind cluster. Nothing
# persists afterward.
set -euo pipefail

CLUSTER=monedula-e2e
COMPOSE_PROJECT=mon-e2e-k8s
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_DIR="${REPO_ROOT}/scenarios/clusters/shared-sasl"

echo "==> Tearing down host shared-sasl compose broker"
docker compose -f "${COMPOSE_DIR}/compose.yaml" -p "${COMPOSE_PROJECT}" down -v 2>/dev/null \
  || echo "    (compose stack already down)"

if ! command -v kind >/dev/null 2>&1; then
  echo "skip: kind not installed (compose torn down; nothing else to do)"
  exit 0
fi

echo "==> Deleting kind cluster '${CLUSTER}'"
kind delete cluster --name "${CLUSTER}"
echo "==> Done."
