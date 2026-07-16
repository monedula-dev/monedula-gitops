#!/usr/bin/env bash
# lib.bash -- bats helper library for the Monedula GitOps k8s e2e suite.
#
# Source this file at the top of run.bats:
#   source "$(dirname "$BATS_TEST_FILENAME")/lib.bash"
#
# All helpers assume:
#   - REPO_ROOT is set (the absolute path to the repository root)
#   - E2E_NS is set (the Kubernetes namespace for e2e resources, e.g. monedula-e2e)
#   - KUBECONFIG is set or the ambient kubeconfig is valid for the e2e cluster
#   - The monedula-gitops binary is on PATH (or MONEDULA_BIN is set)

# Resolve the binary to use.  Prefer MONEDULA_BIN if set; otherwise look for
# the binary in $REPO_ROOT/bin/ (the conventional location after `go build`)
# then fall back to PATH.
_mg_bin() {
  if [[ -n "${MONEDULA_BIN:-}" ]]; then
    echo "${MONEDULA_BIN}"
  elif [[ -x "${REPO_ROOT}/bin/monedula-gitops" ]]; then
    echo "${REPO_ROOT}/bin/monedula-gitops"
  else
    echo "monedula-gitops"
  fi
}

# wait_topic_ready <topic-name> [namespace] [timeout-seconds]
#
# Waits for a KafkaTopic CR to reach Ready=True via kubectl wait. Defaults to
# namespace $E2E_NS and timeout 90s.
wait_topic_ready() {
  local name="${1:?wait_topic_ready: topic name required}"
  local ns="${2:-${E2E_NS}}"
  local timeout="${3:-90}"

  kubectl -n "${ns}" wait kafkatopic/"${name}" \
    --for=condition=Ready \
    --timeout="${timeout}s"
}

# delete_topic_and_wait <topic-cr-name> [namespace] [timeout-seconds]
#
# Deletes a KafkaTopic CR and blocks until it is fully gone (the finalizer has
# run). For Orphan this returns quickly (finalizer is a no-op); for Delete it
# blocks until the operator has removed the broker topic + finalizer. Defaults:
# namespace $E2E_NS, timeout 90s.
delete_topic_and_wait() {
  local name="${1:?delete_topic_and_wait: topic name required}"
  local ns="${2:-${E2E_NS}}"
  local timeout="${3:-90}"
  kubectl -n "${ns}" delete kafkatopic "${name}" --wait --timeout="${timeout}s"
}

# apply_scenario <scenario-dir> [namespace]
#
# kubectl applies every manifest in <scenario-dir>/manifests/ into the given
# namespace (default $E2E_NS). Uses --namespace so namespace-less manifests
# land in the right namespace.
#
# If <scenario-dir>/manifests-k8s/ exists it is used instead of manifests/.
# Almost every scenario applies the same manifests/ in both cli and k8s modes;
# scenario 25-kafka-user is the sole exception because its KafkaUser's
# password source is mode-exclusive (CLI: valueFrom.env; operator:
# valueFrom.secretKeyRef/generate — never both from one manifest, see
# internal/validation.ValidateUserShape). This mirrors the CLI orchestrator's
# scenarioManifestsDir helper (test/e2e/cli/runner_test.go).
apply_scenario() {
  local scenario_dir="${1:?apply_scenario: scenario-dir required}"
  local ns="${2:-${E2E_NS}}"

  local manifests_dir="${scenario_dir}/manifests"
  if [[ -d "${scenario_dir}/manifests-k8s" ]]; then
    manifests_dir="${scenario_dir}/manifests-k8s"
  fi

  kubectl -n "${ns}" apply -f "${manifests_dir}/"
}

# mg_check <scenario-dir> [namespace] [extra-args...]
#
# Runs `monedula-gitops e2e check --scenario <dir> --mode k8s --namespace <ns>`
# and prints output. Exits with the binary's exit code.
#
# liveState IS asserted in k8s mode: the operator (in kind) and this host-side
# binary share one broker — the shared-sasl compose broker on the host. The
# operator reaches it as host.docker.internal:9096; this binary reaches the same
# broker via the host cluster.yaml (localhost:9092). So mg_check passes
# --cluster-config (the host shared-sasl manifest) and the SCRAM/SR creds via
# env so e2e check probes both Kubernetes conditions AND broker liveState.
mg_check() {
  local scenario_dir="${1:?mg_check: scenario-dir required}"
  local ns="${2:-${E2E_NS}}"
  shift 2

  local bin
  bin="$(_mg_bin)"

  local kc_args=()
  if [[ -n "${KUBECONFIG:-}" ]]; then
    kc_args=(--kubeconfig "${KUBECONFIG}")
  fi

  local cluster_config="${REPO_ROOT}/scenarios/clusters/shared-sasl/cluster.yaml"

  KAFKA_USERNAME=admin KAFKA_PASSWORD=admin-secret \
  SR_USERNAME=sr SR_PASSWORD=sr-secret \
  "${bin}" e2e check \
    --scenario "${scenario_dir}" \
    --mode k8s \
    --namespace "${ns}" \
    --cluster-config "${cluster_config}" \
    "${kc_args[@]}" \
    "$@"
}

# assert_admission_rejected <scenario-dir> <stderr-file>
#
# Reads the expected message pattern from:
#   <scenario-dir>/expect.yaml  ->  k8s.admission.messageMatches
#
# Then asserts that <stderr-file> contains a line matching that pattern.
# Uses `yq` if available (safe YAML parse), otherwise falls back to grep on the
# raw YAML value (adequate for simple unquoted strings).
#
# Exits non-zero if:
#   - messageMatches is absent from expect.yaml
#   - <stderr-file> does not match the pattern
assert_admission_rejected() {
  local scenario_dir="${1:?assert_admission_rejected: scenario-dir required}"
  local stderr_file="${2:?assert_admission_rejected: stderr-file required}"

  local expect_yaml="${scenario_dir}/expect.yaml"

  local pattern
  if command -v yq >/dev/null 2>&1; then
    pattern="$(yq '.k8s.admission.messageMatches // ""' "${expect_yaml}")"
  else
    # Fallback: extract the value with grep + sed. Handles simple quoted or
    # unquoted YAML strings on a single line.
    pattern="$(grep 'messageMatches' "${expect_yaml}" \
      | sed 's/.*messageMatches:[[:space:]]*//' \
      | tr -d '"')"
  fi

  if [[ -z "${pattern}" ]]; then
    echo "assert_admission_rejected: no k8s.admission.messageMatches in ${expect_yaml}" >&2
    return 1
  fi

  if ! grep -q "${pattern}" "${stderr_file}"; then
    echo "assert_admission_rejected: stderr did not match pattern '${pattern}'" >&2
    echo "--- stderr contents ---" >&2
    cat "${stderr_file}" >&2
    return 1
  fi
}

# cleanup_k8s_scenario <scenario-dir> [namespace]
#
# Runs <scenario-dir>/cleanup.sh k8s in the given namespace context.
# Errors are logged but not fatal (best-effort teardown between tests).
cleanup_k8s_scenario() {
  local scenario_dir="${1:?cleanup_k8s_scenario: scenario-dir required}"
  local ns="${2:-${E2E_NS}}"

  # The per-scenario cleanup.sh runs un-namespaced `kubectl delete` (kubectl has
  # no KUBECTL_DEFAULT_NAMESPACE env var), so point the current context's default
  # namespace at the harness namespace first. The kind kubeconfig is ephemeral
  # (deleted at teardown), so mutating its context here is safe.
  kubectl config set-context --current --namespace="${ns}" >/dev/null 2>&1 || true
  bash "${scenario_dir}/cleanup.sh" k8s || \
    echo "warn: cleanup.sh k8s exited non-zero (scenario: $(basename "${scenario_dir}"))" >&2
}

# scenario_cluster <scenario-dir>
#
# Echoes the `cluster:` profile name from <scenario-dir>/scenario.yaml
# (e.g. "auth-mtls"). Uses yq if present, else a grep/sed fallback.
scenario_cluster() {
  local scenario_dir="${1:?scenario_cluster: scenario-dir required}"
  local f="${scenario_dir}/scenario.yaml"
  if command -v yq >/dev/null 2>&1; then
    yq '.cluster // ""' "${f}"
  else
    grep '^cluster:' "${f}" | sed 's/^cluster:[[:space:]]*//' | tr -d '"'
  fi
}

# setup_auth_profile <profile>
#
# Brings up an auth compose broker on the host with a kind-facing
# host.docker.internal listener, then creates that profile's Secret(s) and a
# k8s-shaped KafkaCluster CR (named "shared") in $E2E_NS so the in-kind operator
# can reconcile against it. The global CoreDNS host.docker.internal alias from
# setup.sh is reused. Profile-specific Secret material is created here; the CR
# itself is the committed scenarios/clusters/<profile>/k8s-cluster.yaml.
setup_auth_profile() {
  local profile="${1:?setup_auth_profile: profile required}"
  local dir="${REPO_ROOT}/scenarios/clusters/${profile}"
  local project="mon-e2e-${profile}"

  # The shared-sasl broker (brought up by setup.sh, project mon-e2e-k8s) holds
  # host port 9092, which every auth broker's CLIENT_HOST listener also needs.
  # Stop it so the auth broker can bind 9092 and the host-side liveState probe
  # (localhost:9092) reaches the auth broker. Auth tests run LAST in the suite
  # (see run.bats), so shared-sasl is not needed again after this.
  docker compose -f "${REPO_ROOT}/scenarios/clusters/shared-sasl/compose.yaml" \
    -p mon-e2e-k8s stop >/dev/null 2>&1 || true

  if [[ "${profile}" == "auth-mds" ]]; then
    # cp-server's rbac-bootstrap one-shot exits 0 and breaks compose --wait, so
    # bring up without --wait and poll MDS authenticate until 200 (mirrors the
    # CLI orchestrator's composeUpMDS).
    docker compose -f "${dir}/compose.yaml" -p "${project}" up -d
    local _i _ready=0
    for _i in $(seq 1 60); do
      if curl -sf -u mds:mds-secret http://localhost:8090/security/1.0/authenticate -o /dev/null; then
        _ready=1; break
      fi
      sleep 2
    done
    (( _ready )) || { echo "setup_auth_profile: MDS did not become ready in 120s" >&2; return 1; }
  else
    docker compose -f "${dir}/compose.yaml" -p "${project}" up -d --wait
  fi

  case "${profile}" in
    auth-mtls)
      kubectl -n "${E2E_NS}" create secret generic kafka-mtls \
        --from-file=ca.crt="${dir}/certs/ca.crt" \
        --from-file=client.crt="${dir}/certs/client.crt" \
        --from-file=client.key="${dir}/certs/client.key" \
        --dry-run=client -o yaml | kubectl apply -f -
      ;;
    auth-oauth)
      kubectl -n "${E2E_NS}" create secret generic kafka-oauth \
        --from-literal=client-id=monedula \
        --from-literal=client-secret=monedula-secret \
        --dry-run=client -o yaml | kubectl apply -f -
      ;;
    auth-mds)
      kubectl -n "${E2E_NS}" create secret generic kafka-mds \
        --from-literal=kafka-username=kafka-admin \
        --from-literal=kafka-password=kafka-admin-secret \
        --from-literal=mds-username=mds \
        --from-literal=mds-password=mds-secret \
        --dry-run=client -o yaml | kubectl apply -f -
      ;;
    auth-sasl-ssl)
      kubectl -n "${E2E_NS}" create secret generic kafka-sasl-ssl \
        --from-file=ca.crt="${dir}/certs/ca.crt" \
        --from-literal=scram-username=admin \
        --from-literal=scram-password=admin-secret \
        --dry-run=client -o yaml | kubectl apply -f -
      ;;
    *)
      echo "setup_auth_profile: unknown profile ${profile}" >&2; return 1 ;;
  esac

  kubectl -n "${E2E_NS}" apply -f "${dir}/k8s-cluster.yaml"
}

# teardown_auth_profile <profile>
#
# Removes the CR + Secret(s) and tears the compose broker down (down -v gives a
# fresh broker per run so state never leaks across runs).
teardown_auth_profile() {
  local profile="${1:?teardown_auth_profile: profile required}"
  local dir="${REPO_ROOT}/scenarios/clusters/${profile}"
  local project="mon-e2e-${profile}"

  kubectl -n "${E2E_NS}" delete -f "${dir}/k8s-cluster.yaml" --ignore-not-found || true
  case "${profile}" in
    auth-mtls) kubectl -n "${E2E_NS}" delete secret kafka-mtls --ignore-not-found || true ;;
    auth-oauth) kubectl -n "${E2E_NS}" delete secret kafka-oauth --ignore-not-found || true ;;
    auth-mds) kubectl -n "${E2E_NS}" delete secret kafka-mds --ignore-not-found || true ;;
    auth-sasl-ssl) kubectl -n "${E2E_NS}" delete secret kafka-sasl-ssl --ignore-not-found || true ;;
    *) echo "teardown_auth_profile: unknown profile ${profile}" >&2; return 1 ;;
  esac
  docker compose -f "${dir}/compose.yaml" -p "${project}" down -v || true
}

# mg_check_auth <scenario-dir> <profile> [namespace]
#
# Like mg_check, but targets an auth profile's host-side cluster-config for the
# liveState probe (with that profile's creds). For auth-mds there is no
# liveState surface (role bindings), so the broker probe is omitted and the
# check is k8s-conditions-only.
mg_check_auth() {
  local scenario_dir="${1:?mg_check_auth: scenario-dir required}"
  local profile="${2:?mg_check_auth: profile required}"
  local ns="${3:-${E2E_NS}}"

  local bin
  bin="$(_mg_bin)"
  local kc_args=()
  if [[ -n "${KUBECONFIG:-}" ]]; then
    kc_args=(--kubeconfig "${KUBECONFIG}")
  fi

  local dir="${REPO_ROOT}/scenarios/clusters/${profile}"

  case "${profile}" in
    auth-mtls)
      # cluster.yaml uses file: refs for the CA + client cert/key; no creds env.
      "${bin}" e2e check --scenario "${scenario_dir}" --mode k8s --namespace "${ns}" \
        --cluster-config "${dir}/cluster.yaml" "${kc_args[@]}"
      ;;
    auth-oauth)
      OAUTH_CLIENT_ID=monedula OAUTH_CLIENT_SECRET=monedula-secret \
      "${bin}" e2e check --scenario "${scenario_dir}" --mode k8s --namespace "${ns}" \
        --cluster-config "${dir}/cluster.yaml" "${kc_args[@]}"
      ;;
    auth-mds)
      # No liveState surface; conditions-only (no --cluster-config).
      "${bin}" e2e check --scenario "${scenario_dir}" --mode k8s --namespace "${ns}" \
        "${kc_args[@]}"
      ;;
    auth-sasl-ssl)
      # Host-side liveState probe via cluster.yaml (localhost:9092 SASL_SSL,
      # CA via file ref; SCRAM via env). The operator uses k8s-cluster.yaml
      # (host.docker.internal:9100, CA via tls.caCert secretKeyRef).
      KAFKA_USERNAME=admin KAFKA_PASSWORD=admin-secret \
      "${bin}" e2e check --scenario "${scenario_dir}" --mode k8s --namespace "${ns}" \
        --cluster-config "${dir}/cluster.yaml" "${kc_args[@]}"
      ;;
    *)
      echo "mg_check_auth: unknown profile ${profile}" >&2; return 1 ;;
  esac
}
