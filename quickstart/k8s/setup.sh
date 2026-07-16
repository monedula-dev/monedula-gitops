#!/usr/bin/env bash
# setup.sh -- stand up the Monedula k8s quickstart end to end:
#   k3d cluster -> cert-manager -> in-cluster Kafka + Schema Registry -> operator -> sample CRs.
#
# Prereqs: docker, k3d, kubectl, helm on PATH. See README.md.
#
# WEBHOOK MODE (default): cert-manager is installed and the operator runs with
# --enable-webhooks.  The ValidatingWebhookConfiguration rejects duplicate
# KafkaTopic identities and topicName renames at admission time.
#
# NO-WEBHOOK MODE: skip cert-manager and the webhook resources by passing the
# environment variable MONEDULA_WEBHOOKS=false:
#
#   MONEDULA_WEBHOOKS=false ./setup.sh
#
set -euo pipefail

# Resolve paths regardless of where the script is invoked from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

CLUSTER=monedula
IMAGE=monedula/monedula-gitops:latest
OPERATOR_NS=monedula-system
OPERATOR_DEPLOY=monedula-gitops

# cert-manager version pinned for reproducibility.
# https://github.com/cert-manager/cert-manager/releases
CERT_MANAGER_VERSION=v1.16.3
CERT_MANAGER_URL="https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"

# MONEDULA_WEBHOOKS defaults to true; set to 'false' to skip cert-manager and
# deploy the operator without the admission webhook (no certs required).
MONEDULA_WEBHOOKS="${MONEDULA_WEBHOOKS:-true}"

TOTAL_STEPS=6
if [[ "${MONEDULA_WEBHOOKS}" != "true" ]]; then
  TOTAL_STEPS=5
fi

echo "==> [1/${TOTAL_STEPS}] Ensuring k3d cluster '${CLUSTER}' exists"
if k3d cluster list | grep -q "^${CLUSTER}\b"; then
  echo "    cluster '${CLUSTER}' already exists, reusing it"
else
  k3d cluster create "${CLUSTER}" --wait
fi

echo "==> [2/${TOTAL_STEPS}] Building operator image and importing it into k3d"
docker build -t "${IMAGE}" "${REPO_ROOT}"
k3d image import "${IMAGE}" -c "${CLUSTER}"

STEP=3

if [[ "${MONEDULA_WEBHOOKS}" == "true" ]]; then
  echo "==> [${STEP}/${TOTAL_STEPS}] Installing cert-manager ${CERT_MANAGER_VERSION}"
  kubectl apply -f "${CERT_MANAGER_URL}"
  echo "    waiting for cert-manager webhook to be Available"
  kubectl -n cert-manager wait --for=condition=Available \
    deploy/cert-manager \
    deploy/cert-manager-cainjector \
    deploy/cert-manager-webhook \
    --timeout=120s
  STEP=$((STEP + 1))
fi

echo "==> [${STEP}/${TOTAL_STEPS}] Deploying in-cluster Kafka + Schema Registry"
kubectl apply -f "${SCRIPT_DIR}/kafka/"
echo "    waiting for Kafka StatefulSet rollout"
kubectl -n kafka rollout status statefulset/kafka --timeout=180s
echo "    waiting for SCRAM bootstrap Job to complete"
kubectl -n kafka wait --for=condition=complete job/kafka-scram-bootstrap --timeout=180s
echo "    waiting for Schema Registry rollout"
kubectl -n kafka rollout status deploy/schema-registry --timeout=180s
STEP=$((STEP + 1))

echo "==> [${STEP}/${TOTAL_STEPS}] Installing operator via Helm (CRDs + RBAC + Deployment)"
HELM_WEBHOOK_ARGS=()
if [ "${MONEDULA_WEBHOOKS:-true}" = "true" ]; then
  HELM_WEBHOOK_ARGS=(--set webhook.enabled=true)
fi
helm upgrade --install monedula-gitops "${REPO_ROOT}/charts/monedula-gitops" \
  --namespace "${OPERATOR_NS}" --create-namespace \
  --set image.repository=monedula/monedula-gitops \
  --set image.tag=latest \
  --set image.pullPolicy=IfNotPresent \
  ${HELM_WEBHOOK_ARGS[@]+"${HELM_WEBHOOK_ARGS[@]}"} \
  --wait --timeout 180s
STEP=$((STEP + 1))

echo "==> [${STEP}/${TOTAL_STEPS}] Applying demo namespace, credentials Secret, and sample CRs"
kubectl apply \
  -f "${SCRIPT_DIR}/operator/namespace.yaml" \
  -f "${SCRIPT_DIR}/operator/kafka-credentials.secret.yaml" \
  -f "${SCRIPT_DIR}/operator/samples/"

WEBHOOK_NOTE=""
if [[ "${MONEDULA_WEBHOOKS}" == "true" ]]; then
  WEBHOOK_NOTE="
  # Webhook rejection demo (duplicate topic identity):
  # Apply a second KafkaTopic with the same topicName to see the webhook reject it:
  kubectl -n monedula-demo apply -f - <<EOF
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaTopic
metadata:
  name: orders-dup
  namespace: monedula-demo
spec:
  clusterRef:
    name: demo
  topicName: payments.orders
  partitions: 3
  deletionPolicy: Orphan
EOF
  # Expected: admission webhook error: KafkaTopic ... conflicts with monedula-demo/orders
  # (same topicName \"payments.orders\" on cluster \"demo\")
"
fi

echo "==> Done. Next steps:"
cat <<EOF

  # List the sample resources:
  kubectl -n monedula-demo get kafkaclusters,kafkatopics,kafkaaccesspolicies

  # Inspect the topic (watch .status.phase reach Ready, TopicSynced=True):
  kubectl -n monedula-demo describe kafkatopic orders

  # Watch the topic phase:
  kubectl -n monedula-demo get kafkatopic orders -w

  # Tail the operator logs:
  kubectl -n ${OPERATOR_NS} logs deploy/${OPERATOR_DEPLOY} -f
${WEBHOOK_NOTE}
When finished, run ./teardown.sh to delete the k3d cluster.
EOF
