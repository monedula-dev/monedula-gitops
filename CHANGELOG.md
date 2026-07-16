# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-08-20

First public release.

### Added
- **Operator:** concurrent reconciles are backed by in-process serialization scoped to the true
  consistency unit: all writers on one `(KafkaCluster, substrate)` — the Kafka ACL substrate
  (`KafkaTopic`/`KafkaAccessPolicy` reconciles and finalizers, prunes included) and the MDS
  role-binding substrate (`KafkaRoleBinding`, plus the topic-access→RBAC auto-map) — execute one
  at a time per cluster, while different clusters, the two substrates of one cluster, and the
  non-substrate kinds (`KafkaQuota`, `KafkaUser`, `KafkaCluster`) run concurrently. The
  duplicate-identity gate additionally serializes on a per-`(cluster, kind, identity)` lock, and
  **contested** gate verdicts (a cached rival found, or a CR that has never been `Ready=True`)
  are re-confirmed with an uncached apiserver read before going terminal — never on the healthy
  steady-state path, which stays at zero apiserver round-trips. All recheck error directions
  fail safe (requeue rather than mutate; never destroy state a co-claimant may still need). See
  the rewritten Scaling section in `docs/operator.md`.
- **`import cluster --skip-quotas`** — skips quota reconstruction entirely (no
  `DescribeClientQuotas` call), mirroring `--skip-users`/`--skip-schemas` end-to-end: the
  summary reports `Quotas: skipped (--skip-quotas)` (and `quotasSkipped: true` in `-o yaml`/
  `-o json`) instead of a zero count. This is the escape hatch for Confluent Cloud, which
  rejects quota describes outright — previously an import against Cloud failed with no way
  around it — and for clusters whose quotas are externally managed.
- **Operator:** duplicate broker identities now go terminal instead of silently flapping. Even
  with webhooks off (the default install), the `KafkaTopic`, `KafkaQuota`, `KafkaRoleBinding`,
  and `KafkaUser` reconcilers detect a second CR claiming the same broker identity before
  touching any broker state: the **older** CR (earlier `creationTimestamp`; namespace/name
  tiebreak) keeps reconciling normally, while the newer one gets a terminal
  `ValidationFailed`/`DuplicateIdentity` condition naming the winner and never mutates the
  broker. Deleting the winner lets the loser recover at its next periodic resync. Previously,
  two CRs claiming one identity silently overwrote each other's broker state every resync
  (last-writer-wins). With webhooks enabled, duplicates are still rejected at admission; the
  reconciler check backstops the webhook-off install and the admission cache-lag window.
- **Operator:** new flags — `--resync-interval` (periodic re-check cadence for healthy
  resources, default `5m`, minimum `30s`; every per-kind requeue derives from it),
  `--max-concurrent-reconciles` (exposed, but values >1 are **clamped to 1** with a startup
  warning: the cluster-wide ACL/role-binding views assume reconciles of the same kind are
  serialized), and `--metrics-secure` (the metrics endpoint requires TokenReview
  authentication and SubjectAccessReview authorization). Matching Helm values:
  `resyncInterval`, `maxConcurrentReconciles`, `metrics.secure`, and
  `metrics.serviceMonitor.{interval,scheme,bearerTokenSecret,tlsConfig,relabelings,metricRelabelings}`.
  A new scaling section in `docs/operator.md` documents the serialization rationale and what
  to expect at hundreds of CRs.
- **Operator:** new Prometheus counter `monedula_reconcile_terminal_total{kind,reason}` counts
  terminal reconcile outcomes (`DuplicateIdentity`, `TenancyDenied`, validation failures, …) for
  alerting on resources stuck awaiting human intervention. Labels are kind + reason only, so
  cardinality stays bounded regardless of CR churn.
- **`auth.oauth.tokenEndpointCA`** (a `valueFrom` reference) — a dedicated CA trust anchor for
  the OAUTHBEARER token endpoint, so an identity provider serving TLS from a private CA works
  without touching broker TLS settings. The IdP is a different trust domain than the Kafka
  brokers, so this is deliberately separate from (and never derived from) `tls.caCert`.
- Release images are now **multi-arch**: `linux/amd64` and `linux/arm64`.
- **Docs:** an honest support matrix (Apache Kafka and Confluent Platform are continuously
  CI-validated; Confluent Cloud is untested; MDS/RBAC is a Confluent Platform-only component)
  and day-2 operations guides: multi-environment promotion, dual-backend (`[acl, rbac]`)
  access-revocation semantics, the managed-scope edge for hand-created ACLs, and
  single-owner-per-principal pipeline guidance. Also corrects a wrong claim that MDS/RBAC
  works against Confluent Cloud (Cloud RBAC is a different, unimplemented API).
- **New resource: `KafkaUser`** — declaratively manages a Kafka SCRAM principal (username,
  mechanism, iteration count, and password), supported by the CLI, the Kubernetes operator, the
  admission webhook, and `import cluster`. A `KafkaUser`'s observable identity is
  `(username, mechanism, iterations)` — Kafka's SCRAM-describe API never exposes the password
  itself, so passwords are never part of drift and rotation is purely event-driven instead.
  - **CLI**: `validate`/`diff`/`apply` support `KafkaUser` with identity-only diff, scoped to the
    declared mechanism (an undeclared live user, or an extra live mechanism left over from a
    partially-applied mechanism swap, is invisible — never reported as drift, never pruned).
    Passwords resolve from `valueFrom.env`/`valueFrom.file` at **execute time**, immediately before
    the SCRAM upsert call, and never appear in rendered plan output. A mechanism change compiles to
    one `Medium`-risk operation that upserts the new mechanism and then deletes the old one. The
    new `apply --rotate-passwords` flag re-resolves and re-upserts the password of every declared,
    in-sync `KafkaUser` from its configured source (users with identity drift are rotated
    regardless); it composes with `--dry-run` like any other operation.
  - **Operator**: a `KafkaUserReconciler` manages the SCRAM credential end-to-end. Referenced
    passwords (`valueFrom.secretKeyRef`) rotate when the source Secret's `resourceVersion` changes,
    tracked in `status.appliedPasswordRef`. An operator-only `spec.password.generate: {}` mode
    provisions and owns a Secret named `<name>-kafka-credentials` (created *before* the SCRAM
    upsert, so a crash can't leave an unrecoverable credential); the password is regenerated only
    when that Secret is deleted, and a pre-existing, non-owned Secret at that name is never adopted
    or overwritten. The finalizer deletes the declared mechanism's credential under
    `deletionPolicy: Delete` (the default for this kind) — ungated, since deleting the CR is itself
    the explicit action. New Prometheus gauges `monedula_managed_users` and
    `monedula_kafka_user_drift_detected`.
  - **Webhook**: rejects a duplicate `(cluster, username)` identity and a `clusterRef` or
    resolved-username change (closing the gap the CRD's always-on CEL immutability rule leaves
    around an unset→set update to a non-`metadata.name` value); deletion is never blocked.
  - **Import**: `import cluster` reconstructs one `KafkaUser` manifest per live username (preferring
    `SCRAM-SHA-512` when both mechanisms are present), with a placeholder
    `KAFKA_USER_<NAME>_PASSWORD` env-var reference standing in for the password — Kafka never
    exposes SCRAM passwords, so the import summary carries an explicit unrecoverability warning.
    The connecting principal's own credential is skipped by default (self-lockout risk); pass
    `--include-connecting-user` to include it, or `--skip-users` to skip credential reconstruction
    entirely.
  - **Scenario 25** (`scenarios/25-kafka-user`) exercises the full story end-to-end: a `KafkaUser`
    provisions a SCRAM credential and a `KafkaTopic`'s `access` block grants that principal `Write`,
    asserted against a real broker in both CLI and k8s modes.
- **BREAKING (behavior): identity fields are now enforced immutable by the
  apiserver, with or without webhooks.** Webhook-off installs that previously
  accepted an identity rename (silently orphaning broker state) now get an
  apiserver rejection. `x-kubernetes-validations` CEL rules baked into the CRDs
  enforce `KafkaTopic.spec.topicName` (immutable once set) and
  `spec.clusterRef.name`, `KafkaAccessPolicy.spec.clusterRef.name`,
  `KafkaQuota.spec.entity`, and the `KafkaRoleBinding` identity set
  (`clusterRef.name`, `principal`, `role`, `scope.type`) — the default install
  (webhooks off) can no longer silently orphan broker state by renaming an
  identity field. The topic webhook also gained the `clusterRef` immutability
  check it was inconsistently missing, and the policy/quota webhooks gained
  matching `clusterRef` checks.
- **Operator:** a failed Schema Registry global-compatibility read now
  surfaces as an informational `SchemaRegistryDegraded` condition on the
  affected `KafkaTopic`, instead of silently falling back to the legacy
  first-set classification with no visible signal.
- **Operator:** `SharedACLsRetained`/`SharedRoleBindingsRetained` events now
  name up to three co-owning resources (`Kind/namespace/name`), with an
  "and N more" overflow, instead of just announcing that tuples were retained.
- **Operator:** `KafkaQuota` and `KafkaRoleBinding` gain the same
  `monedula_kafka_quota_drift_detected`/`monedula_managed_quotas` and
  `monedula_kafka_rolebinding_drift_detected`/`monedula_managed_rolebindings`
  Prometheus gauges the other kinds already export, with lifecycle cleanup
  when a resource is deleted.
- Initial public release.
- Declarative management of Kafka **topics**, **ACLs**, **access policies**, **quotas**
  (user + IP), **schemas** (Schema Registry), and **RBAC role bindings** (Confluent MDS) from
  Kubernetes-style YAML manifests.
- **CLI** (`monedula-gitops`): `validate`, `diff`, `verify`, `apply` (with `--dry-run` and opt-in
  `--prune`), `import cluster`, and `doctor` connectivity/readiness checks.
- **Kubernetes operator**: reconciles `KafkaTopic`, `KafkaAccessPolicy`, `KafkaQuota`, and
  `KafkaRoleBinding` custom resources, with validating admission webhooks, reconciliation modes,
  finalizers/deletion policies, and status conditions.
- **Authentication**: SASL/SCRAM, SASL_SSL, mTLS, OAUTHBEARER, and Confluent MDS/RBAC.
- Distribution via prebuilt binaries (GitHub Releases), Homebrew tap, Docker image, and Helm chart.
- Released images are stamped with version/commit/date, and the Helm chart version follows the
  release tag.
- **Schema Registry TLS:** `spec.schemaRegistry.tls` configures private-CA HTTPS to Schema
  Registry, with the same shape as `spec.tls`/`authorization.mds.tls` (`caCert` `valueFrom`,
  optional client cert/key, dev-only `insecureSkipVerify`). SR TLS secrets participate in the
  operator's rotation-watch index like the Kafka/MDS ones.
- **`doctor`** gains an MDS authenticate + list-roles preflight check when `authorization.mds` is
  configured, instead of only proving that a client could be constructed.

### Changed
- **Operator:** Kubernetes Events are now written through the **`events.k8s.io/v1` API** (the
  deprecated corev1 event recorder is gone, and the `.golangci.yml` suppression that deferred
  the migration with it). `kubectl describe` and `kubectl get events` show the same events as
  before (the apiserver mirrors the two groups); what does change observably: each event now
  carries a populated `action` (`Reconcile` on the reconcile path, `Finalize` on the
  deletion/finalizer path), the emitting controller is reported in the
  `reportingController`/`reportingInstance` fields instead of `source.component`, and repeats
  are aggregated as event **series** (`count`/`series.count` semantics differ slightly). The
  controller names are unchanged (`kafkatopic-controller`, …), but a field selector keyed on
  `source.component` must be re-keyed to `reportingController` to keep matching. **RBAC:** the manager ClusterRole now needs
  `events.k8s.io` `events` `create;patch` instead of core-group `events` — regenerated
  `config/rbac/role.yaml` and the Helm chart already carry the change, but hand-rolled RBAC
  must be updated or the operator logs event-write failures (reconciles are unaffected). The
  leader-election Role keeps its core-group events grant (controller-runtime's leader election
  still emits corev1 events).
- **BREAKING (operator):** `--max-concurrent-reconciles` values >1 are now **honored** for all
  six kinds (previously silently clamped to 1 with a startup warning) and **require
  `--leader-elect`** — the serialization that makes concurrency safe is in-process locking, so a
  second active replica would race it. The operator now refuses to start with >1 and no
  `--leader-elect` (exit code 2), and the Helm chart fails at template time when
  `maxConcurrentReconciles > 1` with `leaderElection.enabled=false`. Deployments that were
  passing >1 without leader election previously ran (clamped); they now fail at startup —
  enable leader election or set concurrency back to 1. Note that >1 parallelizes work across
  clusters, across substrates, and for the non-substrate kinds; same-cluster ACL/role-binding
  writers still serialize (see `docs/operator.md` Scaling).
- **Schema Registry / MDS clients:** idempotent **read** requests now retry transient failures
  (HTTP 429/5xx and transport errors) up to 3 attempts with jittered exponential backoff,
  honoring a server's `Retry-After` (capped at 30s) — a blip during a diff no longer fails the
  whole run. **Writes are never retried**: a mutation that reached the server but lost its
  response must not risk being applied twice.
- **Import:** `import cluster` now skips Kafka/Confluent internal housekeeping topics (`__*`
  such as `__consumer_offsets`, plus `_schemas` and `_confluent*`) by default; pass the new
  `--include-internal` flag to import them too.
- **CLI:** the hidden `e2e` command accepts `--cluster-config-file` like every public command;
  `--cluster-config` remains as a deprecated alias.
- **Helm chart:** the `logLevel` value's default moves from `error` to `warn`,
  matching the CLI's default. Pass `--set logLevel=error` to restore the old
  quiet-by-default behavior.
- **Operator:** with `--enable-webhooks`, `/readyz` now also gates on the
  webhook server having started, so a pod is only marked Ready once it can
  actually serve admission requests — previously a premature-Ready pod could
  black-hole CR create/update traffic during rollout, since the webhooks are
  installed with `failurePolicy: Fail`.
- **BREAKING:** `tls.caCertRef` (`{name, key}`, operator-mode only) is replaced by `tls.caCert`
  (a `valueFrom` reference), matching `clientCert`/`clientKey`. The CLI can now resolve a private
  CA from `file`/`env`/`inline`; operator mode continues to use `secretKeyRef`. Migrate:
  `tls.caCertRef: {name: my-ca, key: ca.crt}` → `tls.caCert: {valueFrom: {secretKeyRef: {name: my-ca, key: ca.crt}}}`
  (operator mode), or `tls.caCert: {valueFrom: {file: certs/ca.crt}}` (CLI mode).
  `insecureSkipVerify` remains a documented dev-only escape hatch.
- **BREAKING:** Multi-tenancy is now enforced uniformly across all data-plane kinds. The
  `allowedNamespaces` allow-list now also applies to `KafkaQuota` and `KafkaRoleBinding` (previously
  topic/policy-only), and prefix-restricted namespaces are further constrained: consumer group names
  are prefix-checked like topic names, `KafkaAccessPolicy` rules on `cluster`/`transactionalId`/
  `delegationToken` resources are denied, and cluster-scoped `KafkaRoleBinding`s (e.g. `SystemAdmin`)
  are denied. If you use prefix-restricted namespaces, audit existing group names and cluster-scoped
  role bindings before upgrading — previously-allowed resources may now be rejected with
  `TenancyDenied`.
- **BREAKING (behavior):** `RemoveQuota` is now `RiskMedium`/`GateDestructive` — it authoritatively
  deletes a live quota (unthrottling a client) and was the only such deletion with no approval
  gate. Removing a quota now requires `--allow-destructive` (CLI) or the
  `gitops.monedula.dev/allow-destructive: "true"` annotation (operator), matching
  `LowerSchemaCompatibility`/`DeleteSubject`.
- **BREAKING (behavior):** declaring a subject's compatibility below the Schema Registry's global
  default on its **first** set is now a gated `LowerSchemaCompatibility`, not an ungated Raise —
  the effective starting point for an unset subject is the registry's global default, fetched
  once per run, and the diff/apply `Message` now notes the inherited baseline it was compared
  against. If the global level can't be determined, the previous (ungated Raise) behavior applies.
- **BREAKING (behavior):** `KafkaRoleBinding.spec.deletionPolicy` now defaults to `Delete`
  (previously `Orphan`), matching `KafkaAccessPolicy` and `KafkaUser` — the compiled MDS role
  bindings are this CR's entire reason to exist. Deleting a `KafkaRoleBinding` that omits
  `deletionPolicy` now removes its MDS role binding(s) (subject to the co-ownership shield: a
  binding still desired by another live CR is retained). This is **ungated** (no `allow-delete`
  needed), same as `KafkaUser`. To keep the old orphan-on-delete behavior, set
  `deletionPolicy: Orphan` explicitly.
- **CLI:** the default `--log-level` is now `warn` (previously `error`), so role-name typos and
  `RBACCoarsened` warnings are visible without extra flags. Pass `--log-level error` to restore
  the old quiet-by-default behavior.
- **CLI:** `--cluster-config-file` gains the `-c` shorthand.
- Ctrl-C (`SIGINT`) now cancels in-flight Kafka/MDS/Schema Registry admin calls promptly instead
  of blocking until the operation completes; partial results are still rendered where applicable.

### Fixed
- **Operator:** latent cross-kind races between controllers sharing one cluster's ACL or MDS
  state are closed by the new per-`(cluster, substrate)` locks — these existed even at
  reconcile concurrency 1, because different kinds run in separate goroutines. In rare
  interleavings, the `KafkaTopic` and `KafkaAccessPolicy` controllers (or the topic RBAC
  auto-map and the `KafkaRoleBinding` controller, on MDS) could each build their cluster-wide
  view from independent snapshots and transiently revoke an ACL/role binding the other had just
  created (healed at the next resync); in the worst case, a stale prune-consent snapshot could
  **permanently delete an unmanaged, out-of-band ACL** despite a live `prune: false` veto.
  Each view-build→apply span is now atomic per cluster+substrate.
- **CLI:** `diff`/`verify`/`apply` now read live state **only for the kinds the loaded
  manifests declare**, mirroring the gating that already existed for Schema Registry, MDS,
  and SCRAM reads. Previously topics, ACLs, and client quotas were always read up front, so a
  least-privilege credential failed on kinds it never manages — on Confluent Cloud, where API
  keys cannot `DescribeClientQuotas`, the unconditional quota read failed **every** apply with
  `CLUSTER_AUTHORIZATION_FAILED`, even for topic-only manifest sets. A topics-only pipeline now
  needs no ACL/quota describe rights at all. Skipping a read cannot change the computed plan:
  every per-kind diff (including prune candidacy) derives from the desired set, so an
  undeclared kind produces zero operations either way. `doctor` intentionally still probes the
  broker permission surface unconditionally.
- **Loader:** a manifest file passed via `-f` both directly and through its containing
  directory is now loaded once (inputs are deduped by absolute path, regardless of argument
  order) — previously it was loaded twice and tripped a spurious duplicate-identity validation
  error.
- **Operator:** the data-plane controllers (`KafkaTopic`, `KafkaAccessPolicy`,
  `KafkaQuota`, `KafkaRoleBinding`) now watch `KafkaCluster` and reconcile
  promptly when a referenced cluster appears or its spec changes, instead of
  waiting out error backoff (up to ~16m) or the 5-minute resync; status-only
  cluster updates are filtered out so the cluster controller's own resync
  writes don't storm every dependent. The role-binding controller's
  `MDSNotConfigured` branch also requeues on the resync interval as a
  fallback, so adding `authorization.mds` to a cluster can no longer leave
  role bindings permanently wedged.
- **Operator:** tenancy can no longer wedge deletion — the topic webhook skips
  its tenancy check for objects that already have a `deletionTimestamp`, so
  tightening tenancy after a topic exists can never make it undeletable.
- **MDS:** `ListRoleBindings` now only treats a 404 role lookup as "no one holds this role" — any
  other error (401/403/5xx/network) now surfaces instead of being silently skipped, so an MDS auth
  failure or outage can no longer produce a silently wrong empty diff.
- **Loader:** manifest decoding is now strict — unknown fields (e.g. a typo'd `configs:` instead of
  `config:`) now fail loudly instead of being silently dropped and producing a different desired
  state that still passed `validate`.
- **Operator:** finalizer deletion (`deletionPolicy: Delete`) now respects cross-CR ACL/role-binding
  co-ownership — deleting one `KafkaTopic`/`KafkaAccessPolicy`/`KafkaRoleBinding` no longer revokes
  tuples that another live CR still desires. The delete path subtracts still-desired tuples using the
  same cluster-view aggregation the prune path uses, and never falls back to deleting the full set if
  that view cannot be built.
- **Operator:** deleting one half of a duplicate-identity pair can no longer destroy the surviving
  claimant's broker state — the `KafkaUser` and `KafkaQuota` finalizers now skip broker-side cleanup
  (removing only the finalizer, with an `OrphanedToCoClaimant` event) when another live same-kind CR
  still claims the same identity, whichever side is deleted first. Previously, deleting the loser of
  a duplicate `KafkaUser` pair (the natural remediation for `DuplicateIdentity`) deleted the shared
  SCRAM credential and broke the principal's authentication until the winner's next resync; deleting
  the losing duplicate is now the safe remediation.
- **Import:** schema subject reconstruction is now verified like ACL/RBAC import — per-slot subject
  strategies (matching a key subject no longer overwrites the value slot's detected strategy),
  topics with mixed strategies fall back to explicit schema files with a warning, and a round-trip
  verify recompiles each generated manifest's subjects and falls back to explicit representation on
  any mismatch, so import never emits a manifest that would silently mutate the registry on
  `apply`. A key subject whose compatibility diverges from the value subject's now warns instead of
  being silently folded into one compatibility setting.
- **Import:** topic `metadata.name`s that aren't valid Kubernetes object names (Kafka topics with
  underscores or uppercase letters) are now slugged to a DNS-1123-compliant name with deterministic
  disambiguation; `spec.topicName` always carries the real Kafka name. `KafkaQuota` manifest naming
  now distinguishes default-entity sentinels from literal entity names and disambiguates like
  topics, so two distinct quota entities can no longer collide and overwrite each other's output
  file.
- **franz client:** the OAUTHBEARER token fetch is now timeout-bounded (10s) — a hung identity
  provider can no longer block the SASL handshake indefinitely. `ListTopics` now issues a single
  batched `DescribeTopicConfigs` request for all topics instead of one round-trip per topic
  (per-topic error semantics and result ordering are unchanged).

[Unreleased]: https://github.com/monedula-dev/monedula-gitops/commits/main
