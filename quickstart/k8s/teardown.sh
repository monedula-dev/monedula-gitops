#!/usr/bin/env bash
# teardown.sh -- uninstall the operator and delete the k3d cluster created by
# setup.sh. This removes the whole quickstart (cert-manager, Kafka, operator,
# sample CRs); nothing persists after the cluster is deleted.
set -euo pipefail

CLUSTER=monedula

echo "==> Uninstalling operator Helm release"
helm uninstall monedula-gitops --namespace monedula-system --ignore-not-found || true

# CRDs are retained on uninstall (helm.sh/resource-policy: keep). To fully reset
# (this DELETES all KafkaTopic/KafkaCluster/KafkaAccessPolicy CRs cluster-wide):
#   kubectl delete crd kafkatopics.gitops.monedula.dev kafkaclusters.gitops.monedula.dev kafkaaccesspolicies.gitops.monedula.dev

echo "==> Deleting k3d cluster '${CLUSTER}'"
k3d cluster delete "${CLUSTER}"
echo "==> Done."
