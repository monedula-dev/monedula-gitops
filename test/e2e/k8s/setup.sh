#!/usr/bin/env bash
# setup.sh -- idempotent bring-up of the Monedula k8s e2e environment:
#   kind cluster -> cert-manager -> operator (webhooks) -> host compose broker (host.docker.internal:9096)
#
# Prereqs: docker, kind, kubectl, helm on PATH.
# Must be run from the repository root or any path; SCRIPT_DIR + REPO_ROOT are
# resolved from the script's own location so the working directory is irrelevant.
#
# Guard: if kind or kubectl is absent the script prints a skip message and exits
# 0 so the make e2e-k8s target (which also guards on bats) exits cleanly on
# machines that only have bats absent.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

# ---- tool guard ----------------------------------------------------------
if ! command -v kind >/dev/null 2>&1 || ! command -v kubectl >/dev/null 2>&1; then
  echo "skip: kind or kubectl not installed"
  exit 0
fi

# ---- constants -----------------------------------------------------------
CLUSTER=monedula-e2e
IMAGE=monedula-gitops:e2e
OPERATOR_NS=monedula-system
# Namespace used for the e2e scenario resources (KafkaTopic, KafkaCluster CRs).
E2E_NS=monedula-e2e

# cert-manager version pinned for reproducibility.
# https://github.com/cert-manager/cert-manager/releases
CERT_MANAGER_VERSION=v1.16.3
CERT_MANAGER_URL="https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"

# Compose project name for the host-side shared-sasl broker.
COMPOSE_PROJECT=mon-e2e-k8s
COMPOSE_DIR="${REPO_ROOT}/scenarios/clusters/shared-sasl"

# patch_coredns_host_alias makes in-cluster pods resolve host.docker.internal to
# the address the kind node uses to reach the host, so the operator can connect
# to the host compose broker advertised as host.docker.internal:9096.
patch_coredns_host_alias() {
  local node="${CLUSTER}-control-plane"
  local host_ip
  host_ip="$(docker exec "${node}" getent hosts host.docker.internal 2>/dev/null | awk '{print $1}' | head -n1 || true)"
  if [[ -z "${host_ip}" ]]; then
    host_ip="$(docker network inspect kind -f '{{ (index .IPAM.Config 0).Gateway }}' 2>/dev/null || true)"
  fi
  if [[ -z "${host_ip}" ]]; then
    echo "ERROR: could not determine host IP reachable from kind for host.docker.internal" >&2
    return 1
  fi
  echo "    host.docker.internal -> ${host_ip} (patching CoreDNS)"
  local corefile
  corefile="$(kubectl -n kube-system get configmap coredns -o jsonpath='{.data.Corefile}')"
  if grep -q "host.docker.internal" <<<"${corefile}"; then
    echo "    CoreDNS already has a host.docker.internal alias; leaving it"
    return 0
  fi
  awk -v ip="${host_ip}" '
    /^\.:53 \{/ && !done { print; print "    hosts {"; print "        " ip " host.docker.internal"; print "        fallthrough"; print "    }"; done=1; next }
    { print }
  ' <<<"${corefile}" > /tmp/Corefile.monedula
  kubectl -n kube-system create configmap coredns \
    --from-file=Corefile=/tmp/Corefile.monedula \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n kube-system rollout restart deploy/coredns
  kubectl -n kube-system rollout status deploy/coredns --timeout=90s
}

echo "==> [1/7] Ensuring kind cluster '${CLUSTER}' exists"
if kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
  echo "    cluster '${CLUSTER}' already exists, reusing it"
else
  kind create cluster --name "${CLUSTER}" --wait 60s
fi

echo "==> [2/7] Building operator image (docker build -t ${IMAGE} .)"
docker build -t "${IMAGE}" "${REPO_ROOT}"

echo "==> [3/7] Loading operator image into kind cluster"
kind load docker-image "${IMAGE}" --name "${CLUSTER}"

echo "==> [4/7] Installing cert-manager ${CERT_MANAGER_VERSION}"
kubectl apply -f "${CERT_MANAGER_URL}"
echo "    waiting for cert-manager deployments to be Available (up to 120s)"
kubectl -n cert-manager wait --for=condition=Available \
  deploy/cert-manager \
  deploy/cert-manager-cainjector \
  deploy/cert-manager-webhook \
  --timeout=120s

echo "==> [5/7] Installing operator via Helm (webhook enabled)"
# Note: podSecurityContext.runAsUser=65532 is the numeric UID for the distroless
# `nonroot` user. Without it, kind's kubelet rejects the pod because
# `runAsNonRoot: true` requires a numeric UID when the image uses a named user.
helm upgrade --install monedula-gitops "${REPO_ROOT}/charts/monedula-gitops" \
  --namespace "${OPERATOR_NS}" \
  --create-namespace \
  --set image.repository=monedula-gitops \
  --set image.tag=e2e \
  --set image.pullPolicy=IfNotPresent \
  --set webhook.enabled=true \
  --set webhook.certManager.enabled=true \
  --set podSecurityContext.runAsNonRoot=true \
  --set podSecurityContext.runAsUser=65532 \
  --wait --timeout 240s

echo "==> [6/7] Bringing up the host shared-sasl compose broker"
docker compose -f "${COMPOSE_DIR}/compose.yaml" -p "${COMPOSE_PROJECT}" up -d --wait

echo "    patching CoreDNS so pods resolve host.docker.internal"
patch_coredns_host_alias

echo "==> [7/7] Creating e2e namespace, Kafka-credentials Secret, and KafkaCluster CR"
kubectl create namespace "${E2E_NS}" --dry-run=client -o yaml | kubectl apply -f -

# Credentials Secret: the operator's K8sResolver reads this from the resource's
# own namespace (monedula-e2e). Values match the SCRAM users created by the
# shared-sasl compose kafka-init service (admin/admin-secret, sr/sr-secret).
kubectl -n "${E2E_NS}" apply -f - <<'CREDS_EOF'
apiVersion: v1
kind: Secret
metadata:
  name: kafka-credentials
  namespace: monedula-e2e
  labels:
    app.kubernetes.io/part-of: monedula-e2e
type: Opaque
stringData:
  kafka-username: admin
  kafka-password: admin-secret
  sr-username: sr
  sr-password: sr-secret
CREDS_EOF

# k8s-shaped KafkaCluster CR: references the credentials Secret via secretKeyRef
# so the operator's K8sResolver can resolve them. The cluster is named "shared"
# to match the clusterRef.name used by scenario manifests.
kubectl -n "${E2E_NS}" apply -f - <<'KC_EOF'
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaCluster
metadata:
  name: shared
  namespace: monedula-e2e
  labels:
    app.kubernetes.io/part-of: monedula-e2e
spec:
  bootstrapServers: host.docker.internal:9096
  auth:
    mechanism: SCRAM-SHA-512
    scram:
      username:
        valueFrom:
          secretKeyRef:
            name: kafka-credentials
            key: kafka-username
      password:
        valueFrom:
          secretKeyRef:
            name: kafka-credentials
            key: kafka-password
  schemaRegistry:
    endpoint: http://host.docker.internal:8081
    auth:
      type: basic
      username:
        valueFrom:
          secretKeyRef:
            name: kafka-credentials
            key: sr-username
      password:
        valueFrom:
          secretKeyRef:
            name: kafka-credentials
            key: sr-password
KC_EOF

# auth-sasl-ssl TLS cluster: create the kafka-ca and kafka-tls Secrets in
# monedula-e2e and apply the KafkaCluster CR for scenario 04.
echo "    creating kafka-ca / kafka-tls Secrets for auth-sasl-ssl scenario"
CERTS_DIR="${REPO_ROOT}/scenarios/clusters/auth-sasl-ssl/certs"
kubectl -n "${E2E_NS}" create secret generic kafka-ca \
  --from-file=ca.crt="${CERTS_DIR}/ca.crt" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "${E2E_NS}" create secret generic kafka-tls \
  --from-file=ca.crt="${CERTS_DIR}/ca.crt" \
  --from-file=server.crt="${CERTS_DIR}/server.crt" \
  --from-file=server.key="${CERTS_DIR}/server.key" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "    NOTE: scenario 04-sasl-ssl runs against the shared-sasl broker too."
echo "          The shared broker now runs on the host via docker-compose (SASL_PLAINTEXT),"
echo "          reached by the operator as host.docker.internal:9096. Scenario 04 resolves"
echo "          the KafkaCluster CR name 'shared' from its manifest, so the same broker"
echo "          serves it; the TLS secrets are pre-created above for completeness but the"
echo "          shared broker is SASL_PLAINTEXT (a dedicated TLS broker is a later increment)."

echo ""
echo "==> Setup complete. kind cluster '${CLUSTER}' is ready."
echo "    E2E namespace: ${E2E_NS}"
echo "    Shared broker: host compose (project ${COMPOSE_PROJECT}, host.docker.internal:9096)"
echo "    Operator namespace: ${OPERATOR_NS}"
echo ""
echo "    Run the bats suite:     bats test/e2e/k8s/run.bats"
echo "    Tear down:              test/e2e/k8s/teardown.sh"
