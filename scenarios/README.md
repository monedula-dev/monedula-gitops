# Scenarios catalog

Each directory under `scenarios/` is a self-contained test scenario for the
Monedula GitOps operator. The same declarative contract drives both the
human-readable README and the machine-checked e2e test.

## Scenario index

| Directory | Modes | Cluster profile | What it teaches |
|---|---|---|---|
| `01-create-topic` | cli, k8s | shared-sasl | Simplest declarative topic: a KafkaTopic manifest lands on the broker with its config |
| `02-invalid-manifest` | cli | shared-sasl | `validate` catches a malformed manifest (partitions=0) before any cluster contact |
| `03-webhook-reject-duplicate-identity` | k8s | shared-sasl | Admission webhook rejects a second KafkaTopic claiming the same (cluster, topicName) identity |
| `04-sasl-ssl` | cli, k8s | auth-sasl-ssl | Connecting over SASL_SSL (SCRAM-over-TLS) works end-to-end |
| `05-topic-with-access` | cli, k8s | shared-sasl | A KafkaTopic's inline `access` block compiles to producer/consumer ACLs on the broker |
| `06-access-policy` | cli, k8s | shared-sasl | A standalone KafkaAccessPolicy grants principals access; `AccessPolicySynced` reflects it |
| `07-user-quota` | cli, k8s | shared-sasl | A KafkaQuota sets user producer/consumer byte-rate limits; `QuotaSynced` reflects it |
| `08-ip-quota` | cli, k8s | shared-sasl | A KafkaQuota sets an IP connection-creation-rate limit |
| `09-register-schema` | cli, k8s | shared-sasl | A schema is registered in Schema Registry with a subject-level compatibility level |
| `10-drift-detect-reconcile` | cli | shared-sasl | The detect→converge loop: apply, drift the broker out-of-band, `verify` fails, re-apply, `verify` clean |
| `11-reconciliation-modes` | cli | shared-sasl | An `ObserveOnly` topic reports drift but never fails `verify` (contrast with default Enforce) |
| `12-opt-in-prune` | cli | shared-sasl | An in-scope, no-longer-desired ACL is deleted only under `--prune` (safe by default) |
| `13-topic-deletion-orphan` | k8s | shared-sasl | deletionPolicy Orphan: deleting the CR leaves the Kafka topic on the broker |
| `14-topic-deletion-delete` | k8s | shared-sasl | deletionPolicy Delete + allow-delete annotation: the finalizer removes the Kafka topic |
| `15-tenancy-denied` | k8s | shared-sasl | A KafkaCluster tenancy prefix policy; the admission webhook rejects a violating topic |
| `16-import-round-trip` | cli | shared-sasl | Apply, `import` the live cluster, and `verify` the imported manifests are drift-free (faithful round-trip of topics/ACLs/quotas) |
| `17-diff-dry-run` | cli | shared-sasl | `diff` and `apply --dry-run` are read-only previews (planned create before apply, clean after) |
| `18-schema-evolution` | cli | shared-sasl | A BACKWARD-compatible schema change is accepted; an incompatible one is rejected by the registry |
| `19-drift-ignore-fields` | cli | shared-sasl | `drift.ignoreFields` excludes a config key from drift, so `verify` stays clean after it is mutated |
| `20-mtls` | cli, k8s | auth-mtls | Connect with a mutual-TLS client certificate and apply a topic |
| `21-oauthbearer` | cli, k8s | auth-oauth | Authenticate with an OIDC client-credentials bearer token and apply a topic |
| `22-rolebinding` | cli, k8s | auth-mds | Apply a KafkaRoleBinding and have it created + verified on a real Confluent MDS (RBAC) |
| `23-schema-import-round-trip` | cli | shared-sasl | `import` reconstructs schema subjects (subject, body, compatibility) drift-free: apply an inline AVRO schema, import with schemas, verify-clean |
| `24-doctor-preflight` | cli | shared-sasl | `doctor` runs read-only connectivity/readiness probes (config, kafka-connect/admin, acl-read, schema-registry); a healthy cluster exits 0 |
| `25-kafka-user` | cli, k8s | shared-sasl | A KafkaUser provisions a SCRAM credential; a KafkaTopic's `access` block grants that principal Write — the full credential+ACL onboarding story |

## Scenario convention

Every scenario directory contains:

- `scenario.yaml` — metadata: `title`, `modes`, `cluster`, `summary`
- `manifests/` — the Kubernetes manifests to apply
- `expect.yaml` — the declarative contract: CLI exit codes, k8s conditions, admission assertions, live Kafka state
- `README.md` — human explanation of the teaching point, commands, and expected outcome
- `cleanup.sh` — removes any state the scenario created; accepts a `<cli|k8s>` argument

`manifests/` is applied identically in both cli and k8s modes for every
scenario except `25-kafka-user`, which uses `manifests-cli/` and
`manifests-k8s/` instead: a KafkaUser's password source is mode-exclusive (CLI
resolves `valueFrom.env`/`.file`; the operator resolves
`valueFrom.secretKeyRef` or `generate`), so no single manifest resolves in
both runtimes. The harness (`test/e2e/cli` and `test/e2e/k8s/lib.bash`) picks
the mode-specific directory when present, falling back to `manifests/`
otherwise — see that scenario's README for the full rationale.

## How to run

> **Prerequisite: a running Docker daemon.** Both suites stand up real Kafka
> (and, per profile, Schema Registry, Confluent Server/MDS, a mock OIDC server,
> and OpenLDAP) via Docker Compose. The k8s suite additionally needs `kind`,
> `kubectl`, and `bats`. Start Docker (and your kind toolchain) before running.

### CLI mode (binary + cluster config)

```bash
go test -tags e2e -count=1 ./test/e2e/cli/
```

A real run pulls broker images and brings up a Compose stack per profile, so it
takes several minutes. Note: `go test` prints a bare `ok` for any package that
exits 0, so use `-count=1` to bypass the result cache when you actually want the
scenarios to execute.

**Docker is required and the suite fails fast without it.** If the Docker daemon
is not reachable, the CLI suite exits non-zero with a clear message rather than a
misleading `ok` — an explicit `-tags e2e` run is asking to run the scenarios.
Set `MONEDULA_E2E_SKIP_WITHOUT_DOCKER=1` to skip cleanly instead (for
environments that intentionally run without Docker). Plain `go test ./...` is
unaffected — this suite is behind the `e2e` build tag and is not compiled there.

### k8s mode (operator + cluster)

```bash
make e2e-k8s
```

Runs the bats suite over a `kind` cluster. It skips cleanly (exit 0) when `kind`,
`kubectl`, or `bats` is absent; with the toolchain present, Docker must be
running for the in-kind operator to reach the host Compose brokers.

## Cluster profiles

Cluster profiles live in `scenarios/clusters/`. Each profile directory contains a
`cluster.yaml` with broker address, protocol, and credentials for a specific test
cluster: `shared-sasl` (SASL_PLAINTEXT/SCRAM, the default), `auth-sasl-ssl`
(SCRAM-over-TLS, with dev certs), `auth-mtls` (mutual-TLS client-cert auth), and
`auth-oauth` (SASL_PLAINTEXT/OAUTHBEARER validated against a mock OIDC server), and
`auth-mds` (cp-server + LDAP + the Metadata Service for RBAC role bindings).

## Catalog growth

The catalog now spans topic creation, validation and admission, SASL_SSL,
access control (ACLs and policies), quotas, schema registration, the
drift/reconcile loop (detect→converge, ObserveOnly, opt-in prune), the
deletion lifecycle + multi-tenancy (Orphan vs Delete, tenancy admission), and
the import round-trip (apply → import → verify-clean) — completing the original
scenarios-growth roadmap — plus functional-gap coverage (diff/dry-run preview,
schema-compatibility evolution, drift.ignoreFields) and the full auth-profile set
(SASL/SCRAM, SASL_SSL, mTLS, OAUTHBEARER, and MDS/RBAC via cp-server + LDAP).
All three auth-profile scenarios (mTLS, OAUTHBEARER, MDS/RBAC) now run in both
CLI and k8s modes: each auth broker exposes a dual `host.docker.internal:<port>`
listener for the in-kind operator, with per-profile bats bring-up handled by
`setup_auth_profile`/`mg_check_auth` in `test/e2e/k8s/lib.bash`.
Scenario `23-schema-import-round-trip` extends the import round-trip coverage to
schema subjects: it applies a topic with an inline AVRO schema, imports the live
cluster with schemas enabled, and verifies the imported manifests are drift-free
against a real Schema Registry — proving subject name, schema body, and BACKWARD
compatibility survive the round-trip and closing scenario 16's `--skip-schemas` gap.
Scenario `24-doctor-preflight` (cli, shared-sasl) adds a `doctor` runner step that
runs the read-only `monedula-gitops doctor` connectivity/readiness command against a
healthy cluster and asserts exit 0 with all checks passing (`kafka-admin`,
`schema-registry`, `Doctor: healthy`). This completes the deferred scenarios roadmap.
Scenario `25-kafka-user` (cli, k8s, shared-sasl) extends the catalog to the
KafkaUser kind added in v0.35: it declares a SCRAM-SHA-512 credential for
`svc-orders-app` alongside a `KafkaTopic` whose `access` block grants that
principal `Write`, asserting the live credential (`liveState.users`, backed by
`ListScramCredentials`) and the derived ACL together against a real broker —
the full identity-provisioning-plus-authorization story in one scenario. It is
the first (and, by design, only) scenario with per-mode manifests, since a
KafkaUser's password source cannot be shared across CLI and operator mode
(see the scenario convention note above and its own README).
