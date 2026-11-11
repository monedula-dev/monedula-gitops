# Mirror topics, Phase 2 (operator) Implementation Plan

**Goal:** Run `KafkaMirrorTopic` in the Kubernetes operator (controller, webhook, status, metrics, Helm/RBAC), share one destination-topic identity between `KafkaTopic` and `KafkaMirrorTopic`, guard `KafkaTopic` reconcile with the mirror classification, and make the `KafkaTopic` `deletionPolicy: Delete` finalizer mirror-aware and fail-closed.

**Architecture:** The operator reuses the Phase 1 building blocks unchanged in meaning: `kafkarest` client, `mirror.ReadSnapshot`/`Classify`, `diff.computeMirrorOps`, `mirror.Writer` and executor statuses. A new `reconcile.ReconcileMirrorTopic` core feeds them from a `KafkaMirrorTopicReconciler`. A cross-kind claim resolver in `internal/operator/controller` replaces the per-kind topic duplicate scan, with one shared identity lock key for both kinds. The `KafkaTopic` finalizer gains a pre-client decision ladder (orphan, co-claimant, approval) and a REST preflight plus a bounded, no-retry final verification inside the ACL substrate lock.

**Tech Stack:** Go, controller-runtime, controller-gen, envtest, Helm.

**Spec:** `docs/design/2026-10-20-mirror-topics-design.md` (v7), sections "Kubernetes operator", "Operator concurrent admission", "KafkaTopic mirror guard", "KafkaTopic `deletionPolicy: Delete` finalizer guard (v6)", "Lifecycle state and stalled transitions". Phase 1 plan (for vocabulary): `docs/design/2026-10-21-mirror-topics-phase1-cli-plan.md`. Branch `feat/mirror-topics-operator`, stacked on `feat/mirror-topics` (PR #25).

## Global Constraints

- Module `github.com/monedula-dev/monedula-gitops`. Tests: `go test ./...`; envtest: `go test -tags envtest ./internal/operator/...` (skips without `KUBEBUILDER_ASSETS`). Regenerate with `make generate` and `make manifests`; where `make` is unavailable run the Makefile commands directly.
- Commit messages: conventional, no AI attribution or tool trailers.
- Deleting a `KafkaMirrorTopic` CR **always orphans** the Kafka topic; no REST delete is ever called. A finalizer must never block on Kafka or perform a deletion.
- Canonical claim key: `(resolved KafkaCluster namespace/name, effective topic name)`, where the cluster namespace is `ClusterNamespace` when set, else the resource's namespace. Not kind-qualified. Same key for scan scope, comparator, lock and recheck.
- Winner order (both kinds): earlier `creationTimestamp`; then smaller `namespace/name`; then `KafkaTopic` over `KafkaMirrorTopic`.
- A positive deletion co-claimant result means **full orphan** (no topic, ACL or schema cleanup), decided **before** any Kafka/REST client is built. A failed claim scan is never a negative result.
- Kafka Admin and REST cluster IDs must be non-empty and equal before REST data is used (already enforced by `mirror.ReadSnapshot`/`NewSnapshot`).
- `KafkaTopic` finalizer: no identity lock; preparation outside the ACL substrate lock; final verification under it with one total deadline (default **5s**), no retries, no backoff, no `Retry-After` sleep; `Mirror`, `StoppingMirror`, `Unknown`, timeout or binding failure blocks all further cleanup; `Absent` completes the topic step without `DeleteTopic`.
- Stalled transition threshold: operator setting, default **30m**; `PENDING_STOPPED` is drift immediately regardless of duration.
- Clusters without `spec.rest`: `KafkaTopic` reconcile and finalizer keep today's behavior (plus the co-claimant orphan rule); `KafkaMirrorTopic` reports `RESTNotConfigured`.
- This Windows checkout has `core.autocrlf=true`: `TestWebhookManifestContent` and the internal/output goldens fail on CRLF locally; CI is authoritative. Check gofmt on committed content (`git show HEAD:<file> | gofmt -l`), and never write a CEL empty-string literal in `types.go`.

## Decisions this plan makes (spec leaves them to the builder)

- `KafkaMirrorTopic` has **no finalizer** at all: nothing needs cleanup, and metrics are forgotten on NotFound like `KafkaUser`. This makes "deletion never blocks" structural.
- Shared identity lock: both kinds call `lockIdentity(r.Locks, &cluster, topicIdentityKind, name)` with `const topicIdentityKind = "topic"` (replacing the `"KafkaTopic"` kind string at the KafkaTopic call site).
- Stall detection uses the persisted `status.lastObservedStatusChangeTime` (updated whenever `observedMirrorStatus` changes), not REST `state_time_ms`. A new non-executable action `MirrorTopicStalled` (executor status `Unsupported`) replaces `MirrorTopicProgressing` once the threshold has passed.
- `RESTNotConfigured`: terminal `ValidationFailed` condition with reason `RESTNotConfigured`, phase `Error`, requeue at resync (the cluster watch also re-triggers when `spec.rest` is added).
- Webhook tenancy for mirrors checks the effective destination name with `tenancy.Check`; the source topic lives on another cluster and is not tenancy-checked.
- Final finalizer verification uses a new targeted `mirror.VerifyTopic` (one `GetTopic` + full `ListMirrors`), not a full `ReadSnapshot`, so the 5s budget does not scale with cluster topic count.
- "No retries" under the lock is expressed with a context flag in `kafkarest` (`kafkarest.WithoutRetry(ctx)`), honored by the confluent client's GET helper.
- Operator flags: `--mirror-stall-threshold` (default `30m`, minimum `1m`) and `--finalizer-verification-timeout` (default `5s`, must be `>0` and `<=5m`), with Helm values `mirrorStallThreshold` and `finalizerVerificationTimeout`.

## Review Focus

- The referenced `KafkaCluster` is deleted or unreachable when a `KafkaMirrorTopic` CR is deleted: the CR must disappear immediately with no Kafka/REST contact. Test owned by Task 5.
- With `ClusterNamespace` set (global clusters), a `KafkaTopic` in namespace `a` and a `KafkaMirrorTopic` in namespace `b` for the same cluster/topic: exactly one wins; without `ClusterNamespace` (namespace-local) they are independent. Test owned by Task 2.
- Operator restart while a mirror sits in `PENDING_SYNCHRONIZE`: the stall clock must continue from the persisted time, not restart. Test owned by Task 4.
- REST credentials Secret rotated: the next reconcile must authenticate with the new value (clients are rebuilt per reconcile; the Secret watch makes it prompt). Test owned by Task 1.
- `KafkaTopic` with policy `Orphan` (or force-removal) deleted while the cluster's REST endpoint is down: the finalizer is removed without contacting Kafka or REST. Test owned by Task 8.

---

### Task 1: REST client readiness for a long-running operator

**Files:**
- Modify: `internal/kafkarest/client.go`, `internal/kafkarest/confluent/client.go`
- Modify: `internal/operator/controller/kafkacluster_controller.go` (`ClientFactory`, `DefaultClientFactory`), `internal/operator/controller/kafkacluster_controller_test.go` (`stubFactory`), `internal/operator/controller/secret.go` (`clusterSecretNames`)
- Test: `internal/kafkarest/confluent/client_concurrency_test.go`, `internal/operator/controller/rest_factory_test.go`, `internal/operator/controller/secret_test.go`

**Interfaces:**
- Produces:
  - `func WithoutRetry(ctx context.Context) context.Context` and `func RetryDisabled(ctx context.Context) bool` in `kafkarest`.
  - confluent client: GETs use `httpretry.Do(..., idempotent: !kafkarest.RetryDisabled(ctx))`; cluster-ID cache guarded by a `sync.Mutex` (cached only on success); `http.Client.CheckRedirect` returns `http.ErrUseLastResponse` (a redirect is a non-2xx error).
  - `ClientFactory.RESTFor(ctx context.Context, c *v1alpha1.KafkaCluster) (kafkarest.Client, error)`; `DefaultClientFactory.RESTFor` = `cluster.BuildKafkaRESTClient(c, &operator.K8sResolver{Client: f.Client, Namespace: c.Namespace, Ctx: ctx})` (nil, nil without `spec.rest`); `stubFactory` gains `rest kafkarest.Client; restErr error`.
  - `clusterSecretNames` includes `spec.rest.auth` (username/password/token) and `spec.rest.tls` (caCert/clientCert/clientKey) Secret refs.

- [ ] **Step 1: Write failing tests**

```go
func TestClusterIDConcurrentDiscovery(t *testing.T)   // 20 goroutines call ClusterID; -race clean; exactly 1 GET /v3/clusters
func TestRedirectIsAnError(t *testing.T)               // server 302 -> other host; ListMirrors returns error containing "302", other host never hit
func TestWithoutRetrySingleAttempt(t *testing.T)       // 503 on GET; ctx=WithoutRetry -> exactly 1 request; plain ctx -> >1 (stub httpretry.Sleep)
func TestRESTForRotatesSecret(t *testing.T)            // fake client Secret password "a" -> server sees Basic "a"; update Secret to "b"; next RESTFor -> "b"
func TestRESTForNilWithoutRest(t *testing.T)
func TestClusterSecretNamesIncludesREST(t *testing.T)  // rest.auth.password + rest.tls.caCert secretKeyRefs listed
```

- [ ] **Step 2: Run** `go test -race ./internal/kafkarest/... ./internal/operator/controller/ -run 'ClusterIDConcurrent|Redirect|WithoutRetry|RESTFor|ClusterSecretNames'` → FAIL.
- [ ] **Step 3: Implement** the items in Interfaces; update every `ClientFactory` implementation (compile errors list them).
- [ ] **Step 4: Run** the same command plus `go test ./internal/operator/...` → PASS.
- [ ] **Step 5: Commit** `feat(operator): build Kafka REST clients per reconcile with retry control`

---

### Task 2: Cross-kind topic claims and shared identity lock

**Files:**
- Create: `internal/operator/controller/topicclaim.go`
- Modify: `internal/operator/controller/duplicate.go` (KafkaTopic gate uses the resolver), `internal/operator/controller/kafkatopic_controller.go:172` (lock kind)
- Test: `internal/operator/controller/topicclaim_test.go`, `internal/operator/controller/duplicate_envtest_test.go` (cross-kind cases)

**Interfaces:**
- Produces:
  - `const topicIdentityKind = "topic"`.
  - `func topicClaimName(obj client.Object) (clusterRef string, topic string, ok bool)` for `*KafkaTopic` (resolved name) and `*KafkaMirrorTopic` (`EffectiveTopicName()`).
  - `func olderTopicClaim(a, b client.Object) bool` — `olderIdentityClaim` order, then kind (`KafkaTopic` first).
  - `func listTopicClaims(ctx context.Context, c client.Reader, clusterRef, topic, namespace string, namespaceLocal bool) ([]client.Object, error)` — lists both kinds (no field index), filters by `clusterRef` and topic name, namespace-scoped when `namespaceLocal`, excludes deleting CRs.
  - `func findOlderTopicClaimant(ctx, c client.Reader, obj client.Object, namespaceLocal bool) (client.Object, error)` (oldest rival preceding `obj`, or nil).
  - `func findLiveTopicCoClaimant(ctx, c client.Reader, obj client.Object, namespaceLocal bool) (client.Object, error)` (any live other claimant, age-blind; used by Task 8).
  - `(*KafkaTopicReconciler).duplicateIdentityGate` uses `findOlderTopicClaimant` with the existing cached-then-`APIReader` recheck; the loser message names the winner's kind (`duplicateIdentityMessage(identity, winnerKind, ns, name)`).

- [ ] **Step 1: Write failing tests**

```go
func TestOlderTopicClaimKindTieBreak(t *testing.T)  // same ts, same ns/name: KafkaTopic precedes KafkaMirrorTopic; reverse false
func TestListTopicClaimsBothKinds(t *testing.T)     // fake client: topic + mirror same cluster/topic -> both; different cluster -> excluded; deleting -> excluded
func TestListTopicClaimsNamespaceScope(t *testing.T) // namespaceLocal=true excludes other namespace; false includes it
func TestEnvtestTopicLosesToOlderMirror(t *testing.T) // mirror created first, topic second -> topic Phase Error, ValidationFailed/DuplicateIdentity, message contains "KafkaMirrorTopic"
func TestEnvtestGlobalClusterCrossNamespace(t *testing.T) // ClusterNamespace="infra": topic ns a, mirror ns b -> exactly one winner
```

- [ ] **Step 2–4:** run `go test ./internal/operator/controller/ -run 'TopicClaim|ListTopicClaims'` and the envtest variant → FAIL; implement; → PASS (all existing duplicate tests still pass).
- [ ] **Step 5: Commit** `feat(operator): resolve topic claims across KafkaTopic and KafkaMirrorTopic`

---

### Task 3: `KafkaMirrorTopic` validating webhook and indexes

**Files:**
- Create: `internal/operator/webhook/kafkamirrortopic_webhook.go`, `internal/operator/webhook/kafkamirrortopic_webhook_test.go`, `internal/operator/webhook/kafkamirrortopic_webhook_envtest_test.go`
- Modify: `internal/operator/index/index.go` (`MirrorTopicClusterRefNameIndex`), `internal/operator/webhook/setup.go` (re-export + `SetupWithManager`), `internal/operator/webhook/kafkatopic_webhook.go` (`checkIdentityUnique` also lists mirrors)
- Modify (generated/hand): `config/webhook/manifests.yaml` (regen), `charts/monedula-gitops/templates/webhook/validatingwebhookconfiguration.yaml` (hand block)

**Interfaces:**
- Consumes: `validation.ValidateMirrorTopicShape`, `tenancy.Check`, `KafkaMirrorTopicSpec.EffectiveTopicName()`.
- Produces: `type KafkaMirrorTopicValidator struct{ Reader client.Reader; ClusterNamespace string }` implementing `admission.Validator[*v1alpha1.KafkaMirrorTopic]`; marker `path=/validate-gitops-monedula-dev-v1alpha1-kafkamirrortopic`, `name=vkafkamirrortopic.gitops.monedula.dev`, `resources=kafkamirrortopics`, `verbs=create;update`.
  - Create/Update: shape errors; Update also rejects changes to `clusterRef.name`, `linkName`, `sourceTopicName`, effective name (messages `"spec.<field> is immutable: <old> -> <new>"`); identity uniqueness against both kinds (index lookups, same `clusterNamespaceFor` logic as the topic webhook); tenancy on the effective name (missing cluster allows, like topics). Delete: always allowed.
  - Topic webhook: `checkIdentityUnique` also rejects a `KafkaTopic` whose resolved name is claimed by a `KafkaMirrorTopic` on the same resolved cluster.

- [ ] **Step 1: Write failing tests** — unit: shape, `deletionPolicy: Delete` rejected, each immutable field, tenancy denial. envtest:

```go
func TestEnvtestMirrorWebhook_CollidesWithTopic_Rejected(t *testing.T)
func TestEnvtestTopicWebhook_CollidesWithMirror_Rejected(t *testing.T)
func TestEnvtestMirrorWebhook_EffectiveNameChange_Rejected(t *testing.T)
func TestEnvtestMirrorWebhook_DeleteAlwaysAllowed(t *testing.T)
```

- [ ] **Step 2–4:** FAIL → implement → `make manifests` (webhook manifest regen) + hand Helm block → PASS, including `TestHelmAndKustomizeWebhookAgree`.
- [ ] **Step 5: Commit** `feat(webhook): validate KafkaMirrorTopic and reject cross-kind topic claims`

---

### Task 4: `ReconcileMirrorTopic` core and stall detection

**Files:**
- Create: `internal/operator/reconcile/mirror.go`, `internal/operator/reconcile/mirror_test.go`
- Modify: `internal/operations/operations.go` (`MirrorTopicStalled`), `internal/executor/executor.go` (`MirrorTopicStalled` → `Unsupported`), `internal/operator/reconcile/reconcile.go` (`kindKafkaMirrorTopic`, `reasonRESTNotConfigured`, `mirrorTarget`)

**Interfaces:**
- Consumes: `mirror.ReadSnapshot`, `mirror.Writer`, `diff.Compute` with `Desired.Mirrors`, `executor.Apply`, `finishDetectOnly`/`finishEnforce`/`applyRetryError`.
- Produces:
  - `func ReconcileMirrorTopic(ctx context.Context, m *v1alpha1.KafkaMirrorTopic, cluster *v1alpha1.KafkaCluster, k kafka.AdminClient, rest kafkarest.Client, now time.Time, stallAfter time.Duration) (v1alpha1.KafkaMirrorTopicStatus, error)`.
  - Order: seed conditions → `defaulting.MirrorTopic` → `ValidateMirrorTopicShape` (terminal `ValidationFailed`) → `tenancy.Check(cluster.Spec.Tenancy, m.Namespace, effectiveName)` (terminal `TenancyDenied`) → `rest == nil` → terminal `ValidationFailed`/`RESTNotConfigured` → `ReadSnapshot(ctx, k, rest, []string{m.Spec.LinkName})` → ops for one `DesiredMirror` (same field mapping as the Phase 1 pipeline, `TopicNameExplicit = m.Spec.TopicName != ""`) → stall substitution → mode handling (DetectOnly/ObserveOnly report; Enforce `executor.Apply(ctx, executor.Clients{Kafka: k, Mirrors: mirror.Writer{Kafka: k, REST: rest}}, ops, executor.Approvals{})`).
  - Status: `ObservedMirrorStatus` = live `mirror_status` (empty when absent/unknown); `LinkName`/`SourceTopicName` from spec; `LastObservedStatusChangeTime` set to `now` when `ObservedMirrorStatus` differs from the previous status value, else kept; `LastCheckedTime = now`; `LastAppliedTime = now` after any executed write.
  - Stall: a `MirrorTopicProgressing` op becomes `MirrorTopicStalled` (message includes status and duration) when `now - LastObservedStatusChangeTime > stallAfter`.
  - Any `StateReadError` op (any mode): Ready=False reason `LiveStateError` and a non-nil error (backoff), never phase Ready.

- [ ] **Step 1: Write failing tests** (kafka mock + kafkarest mock, fixed `now`)

```go
func TestReconcileMirrorCreates(t *testing.T)               // Absent -> REST mock Created() has 1; Ready
func TestReconcileMirrorRESTNotConfigured(t *testing.T)     // rest nil -> ValidationFailed reason RESTNotConfigured, Phase Error, err nil
func TestReconcileMirrorUnhealthyIsDrift(t *testing.T)      // FAILED -> Drift.Detected, Phase Drifted (Enforce), ObservedMirrorStatus "FAILED"
func TestReconcileMirrorStateReadError(t *testing.T)        // ListMirrors fails -> err non-nil, Ready False LiveStateError, Created() empty
func TestReconcileMirrorStatusChangeTime(t *testing.T)      // prev ACTIVE -> now PAUSED: time = now; same status again later: unchanged
func TestReconcileMirrorStallAfterThreshold(t *testing.T)   // prev PENDING_SYNCHRONIZE since now-31m, stallAfter 30m -> MirrorTopicStalled drift
func TestReconcileMirrorStallClockSurvivesRestart(t *testing.T) // status preset (as after restart) PENDING_SYNCHRONIZE since now-10m -> Progressing, time unchanged
func TestReconcileMirrorPendingStoppedImmediateDrift(t *testing.T)
func TestReconcileMirrorDetectOnlyNoWrites(t *testing.T)
func TestReconcileMirrorTenancyDenied(t *testing.T)
```

- [ ] **Step 2–4:** `go test ./internal/operator/reconcile/ ./internal/executor/ ./internal/operations/` → FAIL → implement → PASS.
- [ ] **Step 5: Commit** `feat(operator): reconcile KafkaMirrorTopic with stall detection`

---

### Task 5: `KafkaMirrorTopicReconciler`

**Files:**
- Create: `internal/operator/controller/kafkamirrortopic_controller.go`, `internal/operator/controller/kafkamirrortopic_controller_test.go`, `internal/operator/controller/kafkamirrortopic_envtest_test.go`
- Modify: `internal/operator/controller/status.go` (`controllerKafkaMirrorTopic`, `statusEqualIgnoringTimestamps` case), `internal/operator/controller/clusterwatch.go` (`mapClusterToMirrorTopics`), `internal/operator/index/index.go` (register mirror `spec.clusterRef.name` index if Task 3 did not), `internal/operator/metrics.go` (`SetMirrorTopicDrift`, `DeleteMirrorTopicDrift`, `IncManagedMirrorTopics`, collectors registered), RBAC markers → `config/rbac/role.yaml` (regen) and `charts/monedula-gitops/templates/clusterrole.yaml` (hand, identical rules)

**Interfaces:**
- Consumes: Tasks 1, 2, 4.
- Produces: `type KafkaMirrorTopicReconciler struct { client.Client; Scheme *runtime.Scheme; Clients ClientFactory; Recorder events.EventRecorder; ClusterNamespace string; ResyncInterval time.Duration; StallThreshold time.Duration; MaxConcurrentReconciles int; Locks *locking.Registry; APIReader client.Reader; Now func() time.Time }`.
  - Reconcile: Get (NotFound → forget metrics) → **deleting → return (no finalizer, nothing to do)** → resolve cluster (missing → error status `ClusterNotFound`, return err) → `lockIdentity(r.Locks, &cluster, topicIdentityKind, effectiveName)` → cross-kind duplicate gate (`findOlderTopicClaimant`, cached then `APIReader` when not materialized/loser) → `r.Clients.For` and `r.Clients.RESTFor` (error → `ClientsBuildFailed` status, return err) → `reconcile.ReconcileMirrorTopic(..., r.now(), stall)` → unlock → metrics → `updateStatus` → error: event `ReconcileError`, return err; else `RequeueAfter: resyncInterval(r.ResyncInterval)`.
  - `StallThreshold == 0` means 30m. If an older build left a finalizer on a CR, remove it on deletion.
  - Watches: `For` with `watchEventFilter()`; Secrets via `clustersReferencingSecret` → mirrors whose `clusterRef` matches; KafkaClusters via `mapClusterToMirrorTopics` with `clusterWatchPredicate()`.
  - RBAC markers: `kafkamirrortopics` get;list;watch;update;patch, `kafkamirrortopics/status` get;update;patch, `kafkamirrortopics/finalizers` update, `kafkaclusters` get;list;watch, `secrets` get;list;watch.

- [ ] **Step 1: Write failing tests**

```go
func TestMirrorReconcilerCreatesMirror(t *testing.T)            // stubFactory{k, rest} -> REST Created() 1, status Ready
func TestMirrorReconcilerDeleteNeverContactsCluster(t *testing.T) // CR deleting, stubFactory err set, cluster absent -> result nil error, no factory call
func TestMirrorReconcilerStatusWriteIdempotent(t *testing.T)    // second reconcile with same state -> no Status().Update (statusEqualIgnoringTimestamps)
func TestEnvtestMirrorLosesToOlderTopic(t *testing.T)           // topic first -> mirror ValidationFailed/DuplicateIdentity, no REST calls
func TestEnvtestMirrorDeletionWithClusterGone(t *testing.T)     // delete KafkaCluster, then mirror CR -> CR gone without reconcile error
```

- [ ] **Step 2–4:** FAIL → implement → `make manifests` (role.yaml) + hand clusterrole edit → `go test ./internal/operator/...` (incl. `TestHelmClusterRoleRulesMatchConfig`) and envtest → PASS.
- [ ] **Step 5: Commit** `feat(operator): add KafkaMirrorTopic controller`

---

### Task 6: Manager wiring, operator flags, Helm and sample

**Files:**
- Modify: `internal/operator/manager/manager.go` (`Options.MirrorStallThreshold`, `Options.FinalizerVerificationTimeout`; register mirror reconciler and validator; pass `FinalizerVerificationTimeout` to `KafkaTopicReconciler` for Task 8), `internal/cli/operator.go` (flags, `validateOperatorOptions`, help text listing kinds), `charts/monedula-gitops/values.yaml`, `charts/monedula-gitops/templates/deployment.yaml`
- Create: `config/samples/gitops_v1alpha1_kafkamirrortopic.yaml`, `config/samples/gitops_v1alpha1_kafkacluster_rest.yaml`
- Modify: `CHANGELOG.md` (Unreleased: operator support, the two new flags, and one sentence that `KafkaTopic` `deletionPolicy: Delete` verification is best effort: an out-of-band promote or failover racing the final check cannot be excluded, so `Orphan` stays the default)
- Test: `internal/cli/operator_test.go`, `internal/operator/manager/manager_test.go`

**Interfaces:**
- Produces: flags `--mirror-stall-threshold` (default `30m`, error below `1m`: `"--mirror-stall-threshold must be at least 1m"`) and `--finalizer-verification-timeout` (default `5s`, error when `<=0` or `>5m`: `"--finalizer-verification-timeout must be > 0 and <= 5m"`); Helm values `mirrorStallThreshold: ""` / `finalizerVerificationTimeout: ""` rendered as args only when set (same `with` pattern as `resyncInterval`); `KafkaTopicReconciler.FinalizerVerificationTimeout time.Duration`.

- [ ] **Step 1: Write failing tests** — flag defaults, both validation errors, manager registers `KafkaMirrorTopic` controller (existing manager test pattern), Helm render includes args when values set.
- [ ] **Step 2–4:** FAIL → implement → PASS (`go test ./internal/cli/ ./internal/operator/manager/`).
- [ ] **Step 5: Commit** `feat(operator): wire KafkaMirrorTopic controller and mirror operator flags`

---

### Task 7: Mirror guard in `KafkaTopic` reconcile, and targeted verification helper

**Files:**
- Modify: `internal/operator/reconcile/reconcile.go` (`ReconcileTopic` gains `rest kafkarest.Client`; sets `live.Mirrors` when non-nil), `internal/operator/controller/kafkatopic_controller.go` (calls `RESTFor`; build failure → error status, no topic mutation)
- Create: `internal/mirror/verify.go`, `internal/mirror/verify_test.go`
- Test: `internal/operator/reconcile/topic_mirror_guard_test.go`

**Interfaces:**
- Produces:
  - `func ReconcileTopic(ctx, topic, cluster, k, sr, r secrets.Resolver, view, mdsClient, rbView, rest kafkarest.Client) (v1alpha1.KafkaTopicStatus, error)` — when `rest != nil`, `snap := mirror.ReadSnapshot(ctx, k, rest, nil)` and `live.Mirrors = &snap` before `diff.Compute`; `rest == nil` → unchanged.
  - `func VerifyTopic(ctx context.Context, k kafka.AdminClient, r kafkarest.Client, topic string) (Classification, error)` — Admin and REST cluster IDs (same rules as `NewSnapshot`), `k.GetTopic(ctx, topic)` (nil → absent), full `r.ListMirrors(ctx)`, then classify through `NewSnapshot` with a one-topic map; any read error → `(Unknown, err)`.

- [ ] **Step 1: Write failing tests**

```go
func TestReconcileTopicRejectsLiveMirror(t *testing.T)    // rest mock has ACTIVE mirror "orders" -> no UpdateTopicConfig on kafka mock; Rejected drift in status
func TestReconcileTopicRESTDownBlocks(t *testing.T)       // ListMirrors fails -> err non-nil, no CreateTopic
func TestReconcileTopicNoRESTUnchanged(t *testing.T)      // rest nil -> identical calls to before (golden call list)
func TestVerifyTopicTable(t *testing.T)                   // Absent, Normal, Mirror(other link), PENDING_STOPPED, STOPPED+topic, ID mismatch, ListMirrors error -> expected class/err
func TestVerifyTopicUsesGetTopicNotListTopics(t *testing.T) // kafka mock records calls: no ListTopics
```

- [ ] **Step 2–4:** FAIL → implement (update all `ReconcileTopic` callers and tests) → PASS.
- [ ] **Step 5: Commit** `feat(operator): guard KafkaTopic reconcile against live mirror topics`

---

### Task 8: `KafkaTopic` finalizer decision ladder (before clients)

**Files:**
- Modify: `internal/operator/controller/kafkatopic_controller.go` (deletion branch moves ahead of `r.Clients.For`)
- Test: `internal/operator/controller/kafkatopic_finalizer_test.go`, `internal/operator/controller/deletion_coownership_test.go` (extend)

**Interfaces:**
- Consumes: `findLiveTopicCoClaimant` (Task 2), `effectiveDeletionPolicy`, `AnnotationForceFinalizerRemoval`, `reconcile.AnnotationAllowDelete`.
- Produces, in this order, after the cluster Get succeeds (cluster Get failure keeps `handleUnreachableDeletion`):
  1. effective policy `Orphan` or force-removal annotation → `removeFinalizer` (no clients).
  2. policy `Delete`: co-claimant scan (cached; `APIReader` recheck when the cached scan finds none). Positive → warning event `OrphanedToCoClaimant` naming kind/ns/name, `removeFinalizer`, no cleanup. Scan error → event `DeleteBlocked` reason `ClaimCheckFailed`, keep finalizer, return error.
  3. missing `allow-delete` → existing `DeleteNotApproved` requeue.
  4. only now build clients (`For`, `RESTFor`); failure → `handleUnreachableDeletion`; then Task 9's preflight and `handleDeletionWithClient`.

- [ ] **Step 1: Write failing tests**

```go
func TestFinalizerOrphanSkipsClients(t *testing.T)            // policy Orphan, stubFactory returns err -> finalizer removed, factory never called
func TestFinalizerForceRemovalSkipsClients(t *testing.T)
func TestFinalizerCoClaimantFullOrphan(t *testing.T)          // policy Delete + allow-delete, live KafkaMirrorTopic same topic -> finalizer removed, kafka mock: no DeleteTopic/DeleteACLs, event OrphanedToCoClaimant
func TestFinalizerCoClaimantBeatsRESTOutage(t *testing.T)      // same + stubFactory err -> still orphaned
func TestFinalizerClaimScanErrorBlocks(t *testing.T)          // reader List error -> finalizer kept, err returned, no deletes
```

- [ ] **Step 2–4:** FAIL → implement → PASS (existing finalizer tests still pass).
- [ ] **Step 5: Commit** `feat(operator): decide KafkaTopic orphaning before building clients`

---

### Task 9: `KafkaTopic` finalizer REST preflight and bounded final verification

**Files:**
- Modify: `internal/operator/controller/kafkatopic_controller.go` (`handleDeletionWithClient`, `deleteTopicState`)
- Test: `internal/operator/controller/kafkatopic_finalizer_verify_test.go`

**Interfaces:**
- Consumes: `mirror.VerifyTopic` (Task 7), `kafkarest.WithoutRetry` (Task 1), `KafkaTopicReconciler.FinalizerVerificationTimeout` (Task 6; `0` means 5s).
- Produces:
  - Preflight (outside the ACL lock), only when `rest != nil`: `class, err := mirror.VerifyTopic(ctx, k, rest, name)`. `Mirror`/`StoppingMirror` → event `MirrorDeleteBlocked`, keep finalizer, `RequeueAfter: resync`. `Unknown`/error → event `DeleteBlocked` with the error text, keep finalizer, return error. `Normal`/`StoppedFormerMirror`/`Absent` → continue.
  - `deleteTopicState(ctx, topic, k, sr, resolver, cluster, clusterNS, verify func(context.Context) (mirror.Classification, error), preflight mirror.Classification) error`: `verify` is nil without REST. With REST, inside the ACL substrate lock, run `verify` with `ctx2, cancel := context.WithTimeout(kafkarest.WithoutRetry(ctx), budget)` exactly once: immediately before `DeleteTopic` when preflight was `Normal`/`StoppedFormerMirror`; before the first ACL deletion when preflight was `Absent`. Result `Absent` → skip `DeleteTopic`, continue remaining cleanup; `Normal`/`StoppedFormerMirror` → `DeleteTopic`; preflight `Absent` but now a topic → return error "topic appeared during deletion; retrying from preflight"; anything else or error → return an error naming the stage, and if ACLs were already deleted, a warning event `PartialCleanup` listing the completed stages. Never delete schema subjects after a blocked stage.
  - No identity lock is added; the substrate lock is released on every path (`defer`).

- [ ] **Step 1: Write failing tests**

```go
func TestFinalizerPreflightBlocksLiveMirror(t *testing.T)        // ACTIVE mirror -> no DeleteACLs/DeleteTopic, event MirrorDeleteBlocked
func TestFinalizerPreflightRESTDownBlocks(t *testing.T)
func TestFinalizerFinalVerifyBlocksMirrorAppeared(t *testing.T)  // preflight Normal; verify stub returns Mirror -> no DeleteTopic, PartialCleanup event, schema subjects untouched
func TestFinalizerFinalVerifyTimeout(t *testing.T)               // verify blocks until ctx done; budget 50ms -> returns within ~budget, no DeleteTopic, ACL lock released (a second lockSubstrate succeeds)
func TestFinalizerFinalVerifySingleAttempt(t *testing.T)        // REST mock/httptest 503 -> exactly 1 ListMirrors request
func TestFinalizerAbsentRecoveryAfterRestart(t *testing.T)       // topic already gone, no mirror -> no DeleteTopic, ACL cleanup runs, finalizer removed
func TestFinalizerAbsentThenAppeared(t *testing.T)               // preflight Absent, verify Normal -> error, finalizer kept, no DeleteTopic
func TestFinalizerNoRESTLegacyPath(t *testing.T)                 // rest nil -> today's behavior (golden call list)
```

- [ ] **Step 2–4:** FAIL → implement → PASS (`go test ./internal/operator/...` and envtest).
- [ ] **Step 5: Commit** `feat(operator): verify mirror state before KafkaTopic deletion cleanup`

---

## Out of scope (Phase 3)

Two-cluster `cp-server` e2e profile and scenario `26-mirror-topic`, the k8s bats e2e for mirrors, REST client integration tests against real `cp-server`, compatibility matrix and the Confluent Cloud smoke test; confirming Confluent's `topic.config.sync.include` default and `next`-link format.
