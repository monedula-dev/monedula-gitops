#!/usr/bin/env bats
# run.bats -- Monedula GitOps k8s scenario suite (bats over kind).
#
# Each @test corresponds to one k8s-mode seed scenario.  The suite is intended
# to run AFTER test/e2e/k8s/setup.sh has brought up the kind cluster, installed
# cert-manager, deployed the operator, and started the host shared-sasl compose broker.
#
# Skip guard: if kind or kubectl is absent the suite exits 0 (skip) so
# `make e2e-k8s` on machines without those tools produces a clean skip.
# The Makefile itself also guards on bats before calling this file.
#
# liveState NOTE: the k8s suite runs the operator (in kind) against the host
# shared-sasl compose broker, advertised to the operator as
# host.docker.internal:9096. The same broker is reachable from the host as
# localhost:9092, so mg_check passes --cluster-config and asserts broker
# liveState (topics/ACLs/quotas/subjects) in k8s mode in addition to conditions.

# ---------------------------------------------------------------------------
# File-level setup
# ---------------------------------------------------------------------------

# Resolve the repo root from this file's location regardless of CWD.
REPO_ROOT="$(cd "$(dirname "$BATS_TEST_FILENAME")/../../.." && pwd)"

# Namespace that setup.sh created for e2e scenario resources.
E2E_NS="monedula-e2e"

# Source the bats helper library.
source "$(dirname "$BATS_TEST_FILENAME")/lib.bash"

# Export KUBECONFIG pointing at the kind cluster so kubectl + the check binary
# both use the right cluster. `kind get kubeconfig` writes to stdout; we cache
# it in a temp file once per suite via setup_file.
KUBECONFIG_FILE=""

setup_file() {
  # Suite-level guard: skip when kind or kubectl are absent.
  if ! command -v kind >/dev/null 2>&1 || ! command -v kubectl >/dev/null 2>&1; then
    skip "kind or kubectl not installed"
  fi

  # Retrieve the kubeconfig for the e2e cluster.
  KUBECONFIG_FILE="$(mktemp /tmp/monedula-e2e-kubeconfig.XXXXXX)"
  kind get kubeconfig --name monedula-e2e > "${KUBECONFIG_FILE}"
  export KUBECONFIG="${KUBECONFIG_FILE}"
  export E2E_NS
  export REPO_ROOT
}

teardown_file() {
  if [[ -n "${KUBECONFIG_FILE:-}" && -f "${KUBECONFIG_FILE}" ]]; then
    rm -f "${KUBECONFIG_FILE}"
  fi
}

# ---------------------------------------------------------------------------
# @test 01-create-topic
# ---------------------------------------------------------------------------
# Applies scenarios/01-create-topic/manifests/ into the e2e namespace, waits
# for the KafkaTopic to reach Ready=True, asserts k8s conditions via
# `monedula-gitops e2e check --mode k8s`, and cleans up.
@test "01-create-topic: operator reconciles topic to Ready" {
  local scenario_dir="${REPO_ROOT}/scenarios/01-create-topic"

  # Ensure a clean slate before the test.
  kubectl -n "${E2E_NS}" delete kafkatopic payments-orders --ignore-not-found

  # Apply the scenario manifests.
  apply_scenario "${scenario_dir}" "${E2E_NS}"

  # Wait for the KafkaTopic CR to reach Ready=True (operator reconciled it).
  wait_topic_ready "payments-orders" "${E2E_NS}" 120

  # Assert conditions via the e2e check command.
  # Asserts k8s conditions AND broker liveState (topic config) against the shared compose broker.
  mg_check "${scenario_dir}" "${E2E_NS}"

  # Cleanup: remove the KafkaTopic CR.
  cleanup_k8s_scenario "${scenario_dir}" "${E2E_NS}"
}

# ---------------------------------------------------------------------------
# @test 03-webhook-reject-duplicate-identity
# ---------------------------------------------------------------------------
# Applies a.yaml (KafkaTopic dup-orders-a) which must succeed, then applies
# b.yaml (KafkaTopic dup-orders-b with the same topicName) which must be
# rejected by the validating webhook.  Asserts the rejection message matches
# the pattern in expect.yaml k8s.admission.messageMatches.
@test "03-webhook-reject-duplicate-identity: webhook rejects duplicate topicName" {
  local scenario_dir="${REPO_ROOT}/scenarios/03-webhook-reject-duplicate-identity"

  # Ensure a clean slate before the test.
  kubectl -n "${E2E_NS}" delete kafkatopic dup-orders-a dup-orders-b --ignore-not-found

  # Apply a.yaml: must succeed.
  kubectl -n "${E2E_NS}" apply -f "${scenario_dir}/manifests/a.yaml"

  # Apply b.yaml: must be rejected. Capture combined output (stderr for kubectl),
  # expect non-zero exit. Use `run` so bats captures status without failing.
  local stderr_file
  stderr_file="$(mktemp /tmp/monedula-webhook-stderr.XXXXXX)"
  run bash -c "kubectl -n '${E2E_NS}' apply -f '${scenario_dir}/manifests/b.yaml' 2>&1"
  echo "${output}" > "${stderr_file}"

  # Assert webhook rejected the request (non-zero exit).
  [ "${status}" -ne 0 ]

  # Assert the rejection message matches expect.yaml k8s.admission.messageMatches.
  assert_admission_rejected "${scenario_dir}" "${stderr_file}"
  rm -f "${stderr_file}"

  # Cleanup (a was created, b was rejected).
  cleanup_k8s_scenario "${scenario_dir}" "${E2E_NS}"
}

# ---------------------------------------------------------------------------
# @test 05-topic-with-access
# ---------------------------------------------------------------------------
# Applies scenarios/05-topic-with-access/manifests/ into the e2e namespace.
# Waits for the KafkaTopic acl-orders to reach Ready=True and TopicAccessSynced=True,
# then asserts k8s conditions via `monedula-gitops e2e check --mode k8s`.
# Asserts k8s conditions AND ACL liveState against the shared compose broker.
@test "05-topic-with-access: operator reconciles acl-orders topic and ACLs to Ready" {
  local scenario_dir="${REPO_ROOT}/scenarios/05-topic-with-access"

  # Ensure a clean slate before the test.
  kubectl -n "${E2E_NS}" delete kafkatopic acl-orders --ignore-not-found

  # Apply the scenario manifests.
  apply_scenario "${scenario_dir}" "${E2E_NS}"

  # Wait for the KafkaTopic CR to reach Ready=True.
  kubectl -n "${E2E_NS}" wait kafkatopic/acl-orders \
    --for=condition=Ready \
    --timeout=120s

  # Assert k8s conditions + broker liveState.
  mg_check "${scenario_dir}" "${E2E_NS}"

  # Cleanup.
  cleanup_k8s_scenario "${scenario_dir}" "${E2E_NS}"
}

# ---------------------------------------------------------------------------
# @test 06-access-policy
# ---------------------------------------------------------------------------
# Applies scenarios/06-access-policy/manifests/ into the e2e namespace.
# Waits for the KafkaAccessPolicy billing-access to reach Ready=True and
# AccessPolicySynced=True, then asserts k8s conditions.
# Asserts k8s conditions AND ACL liveState against the shared compose broker.
@test "06-access-policy: operator reconciles billing-access policy to Ready" {
  local scenario_dir="${REPO_ROOT}/scenarios/06-access-policy"

  # Ensure a clean slate before the test.
  kubectl -n "${E2E_NS}" delete kafkaaccesspolicy billing-access --ignore-not-found

  # Apply the scenario manifests.
  apply_scenario "${scenario_dir}" "${E2E_NS}"

  # Wait for the KafkaAccessPolicy CR to reach Ready=True.
  kubectl -n "${E2E_NS}" wait kafkaaccesspolicy/billing-access \
    --for=condition=Ready \
    --timeout=120s

  # Assert k8s conditions + broker liveState.
  mg_check "${scenario_dir}" "${E2E_NS}"

  # Cleanup.
  cleanup_k8s_scenario "${scenario_dir}" "${E2E_NS}"
}

# ---------------------------------------------------------------------------
# @test 07-user-quota
# ---------------------------------------------------------------------------
# Applies scenarios/07-user-quota/manifests/ into the e2e namespace.
# Waits for the KafkaQuota svc-quota to reach Ready=True and QuotaSynced=True,
# then asserts k8s conditions.
# Asserts k8s conditions AND quota liveState against the shared compose broker.
@test "07-user-quota: operator reconciles svc-quota to Ready" {
  local scenario_dir="${REPO_ROOT}/scenarios/07-user-quota"

  # Ensure a clean slate before the test.
  kubectl -n "${E2E_NS}" delete kafkaquota svc-quota --ignore-not-found

  # Apply the scenario manifests.
  apply_scenario "${scenario_dir}" "${E2E_NS}"

  # Wait for the KafkaQuota CR to reach Ready=True.
  kubectl -n "${E2E_NS}" wait kafkaquota/svc-quota \
    --for=condition=Ready \
    --timeout=120s

  # Assert k8s conditions + broker liveState.
  mg_check "${scenario_dir}" "${E2E_NS}"

  # Cleanup.
  cleanup_k8s_scenario "${scenario_dir}" "${E2E_NS}"
}

# ---------------------------------------------------------------------------
# @test 08-ip-quota
# ---------------------------------------------------------------------------
# Applies scenarios/08-ip-quota/manifests/ into the e2e namespace.
# Waits for the KafkaQuota ip-quota to reach Ready=True and QuotaSynced=True,
# then asserts k8s conditions.
# Asserts k8s conditions AND quota liveState against the shared compose broker.
@test "08-ip-quota: operator reconciles ip-quota to Ready" {
  local scenario_dir="${REPO_ROOT}/scenarios/08-ip-quota"

  # Ensure a clean slate before the test.
  kubectl -n "${E2E_NS}" delete kafkaquota ip-quota --ignore-not-found

  # Apply the scenario manifests.
  apply_scenario "${scenario_dir}" "${E2E_NS}"

  # Wait for the KafkaQuota CR to reach Ready=True.
  kubectl -n "${E2E_NS}" wait kafkaquota/ip-quota \
    --for=condition=Ready \
    --timeout=120s

  # Assert k8s conditions + broker liveState.
  mg_check "${scenario_dir}" "${E2E_NS}"

  # Cleanup.
  cleanup_k8s_scenario "${scenario_dir}" "${E2E_NS}"
}

# ---------------------------------------------------------------------------
# @test 09-register-schema
# ---------------------------------------------------------------------------
# Applies scenarios/09-register-schema/manifests/ into the e2e namespace.
# Waits for the KafkaTopic schema-demo to reach Ready=True and SchemaSynced=True,
# then asserts k8s conditions.
# Asserts k8s conditions AND SR-subject liveState against the shared compose broker.
@test "09-register-schema: operator reconciles schema-demo topic and schema to Ready" {
  local scenario_dir="${REPO_ROOT}/scenarios/09-register-schema"

  # Ensure a clean slate before the test.
  kubectl -n "${E2E_NS}" delete kafkatopic schema-demo --ignore-not-found

  # Apply the scenario manifests.
  apply_scenario "${scenario_dir}" "${E2E_NS}"

  # Wait for the KafkaTopic CR to reach Ready=True.
  kubectl -n "${E2E_NS}" wait kafkatopic/schema-demo \
    --for=condition=Ready \
    --timeout=120s

  # Assert k8s conditions + broker liveState.
  mg_check "${scenario_dir}" "${E2E_NS}"

  # Cleanup.
  cleanup_k8s_scenario "${scenario_dir}" "${E2E_NS}"
}

# ---------------------------------------------------------------------------
# @test 25-kafka-user
# ---------------------------------------------------------------------------
# Applies scenarios/25-kafka-user/manifests-k8s/ into the e2e namespace (this
# scenario has per-mode manifests: k8s mode uses password.generate so no
# Secret needs pre-creating — see the scenario README). Waits for both the
# KafkaUser svc-orders-app and the KafkaTopic users-orders to reach Ready=True,
# asserts k8s conditions + broker liveState (credential + ACL), and asserts the
# operator provisioned the generated-password Secret.
@test "25-kafka-user: operator reconciles svc-orders-app credential and users-orders topic to Ready" {
  local scenario_dir="${REPO_ROOT}/scenarios/25-kafka-user"

  # Ensure a clean slate before the test.
  kubectl -n "${E2E_NS}" delete kafkatopic users-orders --ignore-not-found
  kubectl -n "${E2E_NS}" delete kafkauser svc-orders-app --ignore-not-found
  kubectl -n "${E2E_NS}" delete secret svc-orders-app-kafka-credentials --ignore-not-found

  # Apply the scenario manifests (manifests-k8s/: generate-mode password).
  apply_scenario "${scenario_dir}" "${E2E_NS}"

  # Wait for both CRs to reach Ready=True.
  kubectl -n "${E2E_NS}" wait kafkauser/svc-orders-app \
    --for=condition=Ready \
    --timeout=120s
  kubectl -n "${E2E_NS}" wait kafkatopic/users-orders \
    --for=condition=Ready \
    --timeout=120s

  # The operator must have provisioned the generated-password Secret.
  kubectl -n "${E2E_NS}" get secret svc-orders-app-kafka-credentials

  # Assert k8s conditions + broker liveState (credential + ACL).
  mg_check "${scenario_dir}" "${E2E_NS}"

  # Cleanup.
  cleanup_k8s_scenario "${scenario_dir}" "${E2E_NS}"
  kubectl -n "${E2E_NS}" delete secret svc-orders-app-kafka-credentials --ignore-not-found
}

# ---------------------------------------------------------------------------
# @test 13-topic-deletion-orphan
# ---------------------------------------------------------------------------
# Applies an Orphan-policy topic, waits Ready, deletes the CR, then asserts the
# broker topic SURVIVES (liveState.topics) — Orphan leaves Kafka untouched.
@test "13-topic-deletion-orphan: Orphan leaves the broker topic after CR deletion" {
  local scenario_dir="${REPO_ROOT}/scenarios/13-topic-deletion-orphan"

  kubectl -n "${E2E_NS}" delete kafkatopic orphan-demo --ignore-not-found

  apply_scenario "${scenario_dir}" "${E2E_NS}"
  wait_topic_ready "orphan-demo" "${E2E_NS}" 120
  delete_topic_and_wait "orphan-demo" "${E2E_NS}" 90

  # Orphan: the broker topic survives CR deletion (liveState asserts presence).
  mg_check "${scenario_dir}" "${E2E_NS}"

  cleanup_k8s_scenario "${scenario_dir}" "${E2E_NS}"
}

# ---------------------------------------------------------------------------
# @test 14-topic-deletion-delete
# ---------------------------------------------------------------------------
# Applies a Delete-policy topic (with allow-delete annotation), waits Ready,
# deletes the CR, then asserts the broker topic is GONE (liveState.absent) —
# the finalizer removed it.
@test "14-topic-deletion-delete: Delete+approval removes the broker topic" {
  local scenario_dir="${REPO_ROOT}/scenarios/14-topic-deletion-delete"

  kubectl -n "${E2E_NS}" delete kafkatopic delete-demo --ignore-not-found

  apply_scenario "${scenario_dir}" "${E2E_NS}"
  wait_topic_ready "delete-demo" "${E2E_NS}" 120
  delete_topic_and_wait "delete-demo" "${E2E_NS}" 90

  # Delete + allow-delete: the finalizer removed the broker topic (liveState.absent).
  mg_check "${scenario_dir}" "${E2E_NS}"

  cleanup_k8s_scenario "${scenario_dir}" "${E2E_NS}"
}

# ---------------------------------------------------------------------------
# @test 15-tenancy-denied
# ---------------------------------------------------------------------------
# Applies a tenancy-configured KafkaCluster (prefix policy), then attempts a
# topic that violates the prefix; the validating webhook must reject it.
@test "15-tenancy-denied: webhook rejects a topic violating the cluster prefix policy" {
  local scenario_dir="${REPO_ROOT}/scenarios/15-tenancy-denied"

  kubectl -n "${E2E_NS}" delete kafkatopic tenancy-bad --ignore-not-found
  kubectl -n "${E2E_NS}" delete kafkacluster tenant --ignore-not-found

  # Apply the tenancy cluster first; confirm it is readable so the webhook's
  # manager cache is warm (mirrors scenario 03's apply-then-reject two-step).
  kubectl -n "${E2E_NS}" apply -f "${scenario_dir}/manifests/cluster.yaml"
  kubectl -n "${E2E_NS}" get kafkacluster tenant

  # Attempt the violating topic: must be rejected at admission.
  local stderr_file
  stderr_file="$(mktemp /tmp/monedula-tenancy-stderr.XXXXXX)"
  run bash -c "kubectl -n '${E2E_NS}' apply -f '${scenario_dir}/manifests/topic.yaml' 2>&1"
  echo "${output}" > "${stderr_file}"

  [ "${status}" -ne 0 ]

  assert_admission_rejected "${scenario_dir}" "${stderr_file}"
  rm -f "${stderr_file}"

  cleanup_k8s_scenario "${scenario_dir}" "${E2E_NS}"
}

# ===========================================================================
# Auth-profile scenarios (k8s mode) — these run LAST.
# Each calls setup_auth_profile, which STOPS the shared-sasl broker to free
# host port 9092 for the auth broker. After these run, shared-sasl is down, so
# no shared-sasl @test may follow. (Re-running setup.sh restores shared-sasl.)
# ===========================================================================

# ---------------------------------------------------------------------------
# @test 20-mtls (auth profile, k8s mode)
# ---------------------------------------------------------------------------
# Brings up the auth-mtls compose broker (dual listener), creates the mTLS
# Secret + KafkaCluster CR, applies the topic, waits Ready, and asserts k8s
# conditions + host-side liveState (topic present) via the auth-mtls config.
@test "20-mtls: operator reconciles mtls-demo over mutual TLS" {
  local scenario_dir="${REPO_ROOT}/scenarios/20-mtls"

  kubectl -n "${E2E_NS}" delete kafkatopic mtls-demo --ignore-not-found

  setup_auth_profile "auth-mtls"

  apply_scenario "${scenario_dir}" "${E2E_NS}"
  wait_topic_ready "mtls-demo" "${E2E_NS}" 120
  mg_check_auth "${scenario_dir}" "auth-mtls" "${E2E_NS}"

  cleanup_k8s_scenario "${scenario_dir}" "${E2E_NS}"
  teardown_auth_profile "auth-mtls"
}

# ---------------------------------------------------------------------------
# @test 21-oauthbearer (auth profile, k8s mode)
# ---------------------------------------------------------------------------
# Brings up the auth-oauth compose broker (dual listener + per-listener issuer),
# creates the OAuth Secret + KafkaCluster CR, applies the topic, waits Ready,
# and asserts k8s conditions + host-side liveState (the host CLI uses the
# localhost CLIENTHOST listener; the operator uses the host.docker.internal
# CLIENTKIND listener — each validates its own issuer).
@test "21-oauthbearer: operator reconciles oauth-demo over OAUTHBEARER" {
  local scenario_dir="${REPO_ROOT}/scenarios/21-oauthbearer"

  kubectl -n "${E2E_NS}" delete kafkatopic oauth-demo --ignore-not-found

  setup_auth_profile "auth-oauth"

  apply_scenario "${scenario_dir}" "${E2E_NS}"
  wait_topic_ready "oauth-demo" "${E2E_NS}" 120
  mg_check_auth "${scenario_dir}" "auth-oauth" "${E2E_NS}"

  cleanup_k8s_scenario "${scenario_dir}" "${E2E_NS}"
  teardown_auth_profile "auth-oauth"
}

# ---------------------------------------------------------------------------
# @test 22-rolebinding (auth profile, k8s mode)
# ---------------------------------------------------------------------------
# Brings up the auth-mds cp-server broker + MDS (dual listener), creates the MDS
# Secret + KafkaCluster CR, applies the KafkaRoleBinding, waits for it to sync,
# and asserts k8s conditions (conditions-only — role bindings have no liveState
# surface).
@test "22-rolebinding: operator reconciles checkout-read role binding via MDS" {
  local scenario_dir="${REPO_ROOT}/scenarios/22-rolebinding"

  kubectl -n "${E2E_NS}" delete kafkarolebinding checkout-read --ignore-not-found

  setup_auth_profile "auth-mds"

  apply_scenario "${scenario_dir}" "${E2E_NS}"
  kubectl -n "${E2E_NS}" wait kafkarolebinding/checkout-read \
    --for=condition=RoleBindingSynced --timeout=120s
  mg_check_auth "${scenario_dir}" "auth-mds" "${E2E_NS}"

  cleanup_k8s_scenario "${scenario_dir}" "${E2E_NS}"
  teardown_auth_profile "auth-mds"
}

# ---------------------------------------------------------------------------
# @test 04-sasl-ssl (auth profile, k8s mode)
# ---------------------------------------------------------------------------
# Runs against the REAL SASL_SSL broker (SCRAM-SHA-512 over TLS) via the
# auth-sasl-ssl profile, not the shared-sasl SASL_PLAINTEXT broker. setup_auth_profile
# stops the shared-sasl broker (frees port 9092), so this runs in the auth group.
@test "04-sasl-ssl: operator reconciles tls-demo over SASL_SSL" {
  local scenario_dir="${REPO_ROOT}/scenarios/04-sasl-ssl"

  kubectl -n "${E2E_NS}" delete kafkatopic tls-demo --ignore-not-found

  setup_auth_profile "auth-sasl-ssl"

  apply_scenario "${scenario_dir}" "${E2E_NS}"
  wait_topic_ready "tls-demo" "${E2E_NS}" 120
  mg_check_auth "${scenario_dir}" "auth-sasl-ssl" "${E2E_NS}"

  cleanup_k8s_scenario "${scenario_dir}" "${E2E_NS}"
  teardown_auth_profile "auth-sasl-ssl"
}
