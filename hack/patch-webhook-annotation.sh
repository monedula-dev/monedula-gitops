#!/usr/bin/env bash
# patch-webhook-annotation.sh <manifests.yaml>
#
# Idempotently patches the ValidatingWebhookConfiguration produced by
# `controller-gen webhook`:
#
#   1. Replaces the generic placeholder names emitted by controller-gen with the
#      project-specific names (service, namespace, webhook config name).
#   2. Inserts the cert-manager CA-injection annotation so the API server always
#      has the correct CA bundle for the webhook's TLS certificate.
#
# Called by `make manifests` immediately after the webhook generator runs.
# Safe to run multiple times (idempotent).
set -euo pipefail

FILE="${1:?usage: $0 <manifests.yaml>}"

# Declare temp-file vars before the trap so the trap always has valid variable
# references even if mktemp has not yet run.
TMPFILE=""
TMPFILE2=""
trap 'rm -f "${TMPFILE}" "${TMPFILE2}"' EXIT

# Step 1: Replace controller-gen placeholder names.
# BSD sed (macOS) requires -i '' (space + empty string); GNU sed accepts -i ''.
# Use a temp-file approach to be portable.
TMPFILE="$(mktemp)"
sed \
  -e 's/name: validating-webhook-configuration/name: monedula-validating-webhook-configuration/' \
  -e 's/name: webhook-service/name: monedula-gitops-webhook-service/' \
  -e 's/namespace: system/namespace: monedula-system/' \
  "${FILE}" > "${TMPFILE}"
mv "${TMPFILE}" "${FILE}"

# Step 2: Idempotently insert the cert-manager CA-injection annotation block
# after the line '  name: monedula-validating-webhook-configuration'.
if grep -q 'cert-manager.io/inject-ca-from' "${FILE}"; then
  exit 0
fi

TMPFILE2="$(mktemp)"
awk \
  '/^  name: monedula-validating-webhook-configuration$/ {
    print
    print "  annotations:"
    print "    # cert-manager injects the CA bundle from the serving Certificate so the"
    print "    # API server can verify the webhook'"'"'s TLS certificate.  Value:"
    print "    # <namespace>/<Certificate-resource-name>  (must match config/certmanager/)."
    print "    cert-manager.io/inject-ca-from: monedula-system/monedula-gitops-webhook-cert"
    next
  }
  { print }' "${FILE}" > "${TMPFILE2}"
mv "${TMPFILE2}" "${FILE}"

# Post-condition: assert that the expected annotation and project-specific names
# are present.  If controller-gen output structure drifts (e.g. the VWC name
# field moves) these checks will fail loudly rather than silently producing a
# broken manifest.
grep -q 'cert-manager.io/inject-ca-from' "${FILE}" || {
  echo "ERROR: inject-ca-from annotation not inserted — controller-gen output may have drifted" >&2
  exit 1
}
grep -q 'name: monedula-validating-webhook-configuration' "${FILE}" || {
  echo "ERROR: 'monedula-validating-webhook-configuration' name not found — controller-gen output may have drifted" >&2
  exit 1
}
grep -q 'name: monedula-gitops-webhook-service' "${FILE}" || {
  echo "ERROR: 'monedula-gitops-webhook-service' name not found — controller-gen output may have drifted" >&2
  exit 1
}
