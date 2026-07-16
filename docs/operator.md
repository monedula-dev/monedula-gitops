# Kubernetes operator

The `monedula-gitops operator` runs an in-cluster controller that drives Kafka
toward the declared state of custom resources, reusing the same engine as the CLI.

`monedula-gitops operator` runs a [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime)
manager that hosts six reconcilers — one each for `KafkaCluster`,
`KafkaTopic`, `KafkaAccessPolicy`, `KafkaQuota`, `KafkaRoleBinding`, and `KafkaUser` — and drives the cluster toward the
declared state of those custom resources. The operator reconciles through the
**same diff + executor engine** the CLI uses (see [Shared model](cli.md#shared-model)),
so the gating, drift detection, and execution semantics match `apply` exactly.

## Deploying

The operator supports two first-class install methods: Helm (recommended) and
Kustomize. Both ship the same CRDs and RBAC; cert-manager is a prerequisite
only when the admission webhook is enabled.

### Helm

```sh
helm install monedula-gitops oci://ghcr.io/monedula-dev/charts/monedula-gitops \
  --namespace monedula-system --create-namespace
```

The operator image is published to `ghcr.io/monedula-dev/monedula-gitops` on
every `v*` release tag, and the chart defaults to it (tag = chart `appVersion`),
so a bare `helm install` pulls a real image out of the box.

The webhook is **off by default** (cert-free). To enable the admission webhook
(immediate admission-time rejection of identity-uniqueness, tenancy, and ACL
Allow/Deny violations — identity *immutability* is always enforced by CRD
validation rules, and identity *uniqueness* is always enforced
eventually-consistently by the reconcilers, regardless of the webhook, see
[Admission webhook](#admission-webhook); requires cert-manager in the
cluster), add `--set webhook.enabled=true` to the install command above.
Deletion is never blocked by any webhook, for any resource kind — see
[Admission webhook](#admission-webhook).

Key values:

| Value | Default | Purpose |
|---|---|---|
| `image.repository` | `ghcr.io/monedula-dev/monedula-gitops` | Operator image repository. |
| `image.tag` | *(chart appVersion)* | Operator image tag. |
| `webhook.enabled` | `false` | Enable the validating admission webhook (requires cert-manager). |
| `webhook.certManager.enabled` | `true` | Use cert-manager to issue the webhook serving cert. |
| `clusterNamespace` | `""` | Namespace to resolve `KafkaCluster` refs from (empty: each resource's own namespace). |
| `serviceAccount.annotations` | `{}` | Workload-identity annotations (IRSA, GKE WI, Azure WI — pairs with `OAUTHBEARER`). |
| `crds.enabled` | `true` | Render CRDs from the chart. |
| `crds.keep` | `true` | Add `helm.sh/resource-policy: keep` so `helm uninstall` does not delete your CRs. |
| `logLevel` | `warn` | Operator log level (`error`, `warn`, `info`, `debug`). |
| `resyncInterval` | `""` | `--resync-interval` (empty: operator default `5m`; minimum `30s`). |
| `maxConcurrentReconciles` | `""` | `--max-concurrent-reconciles` (empty: operator default `1`). Values >1 require `leaderElection.enabled=true` — the chart refuses to render otherwise. |
| `metrics.secure` | `false` | `--metrics-secure`: require authn/authz (TokenReview/SubjectAccessReview) on the metrics endpoint. |
| `metrics.serviceMonitor.enabled` | `false` | Create a Prometheus Operator `ServiceMonitor`. |
| `metrics.serviceMonitor.interval` | `""` | Scrape interval (e.g. `30s`); empty uses the Prometheus default. |
| `metrics.serviceMonitor.scheme` | `""` | Scrape scheme; empty resolves to `https` automatically when `metrics.secure`, else `http`. |
| `metrics.serviceMonitor.relabelings` / `metricRelabelings` | `[]` | Passed through verbatim to the `ServiceMonitor` endpoint. |

CRDs are included in the chart's `templates/` (not the `crds/` directory) so
that `helm upgrade` keeps them current. `helm uninstall` leaves them in place
by design (`crds.keep: true`), protecting your custom resources.

### Kustomize

Webhook **off** (cert-free):

```sh
# remote (replace vX with the release tag, e.g. v0.8.0)
kubectl apply -k github.com/monedula-dev/monedula-gitops/config/default?ref=vX

# local checkout
kubectl apply -k config/default
```

Webhook **on** (cert-manager must be pre-installed):

```sh
kubectl apply -k github.com/monedula-dev/monedula-gitops/config/overlays/webhook?ref=vX
```

Sample custom resources live under `config/samples/`
(`gitops_v1alpha1_kafkacluster.yaml`, `gitops_v1alpha1_kafkatopic.yaml`,
`gitops_v1alpha1_kafkaaccesspolicy.yaml`) — edit them for your environment and
`kubectl apply -f` them.

## Flags

| Flag                             | Default | Purpose                                                              |
|-----------------------------------|---------|---------------------------------------------------------------------|
| `--metrics-bind-address`         | `:8080` | Address the Prometheus metrics endpoint binds to (`0` disables it).  |
| `--metrics-secure`                | `false` | Require authentication (TokenReview) and authorization (SubjectAccessReview) on the metrics endpoint. Default serves plain HTTP, as in every prior release. |
| `--health-probe-bind-address`    | `:8081` | Address the health/readiness probe endpoint binds to (`/healthz`, `/readyz`). |
| `--leader-elect`                  | `false` | Enable leader election so only one replica is active at a time. Required for `--max-concurrent-reconciles` >1. |
| `--cluster-namespace`             | `""`    | Namespace to resolve `KafkaCluster` refs from (empty: each object's own namespace). |
| `--resync-interval`               | `5m`    | Periodic resync cadence for every reconciler (minimum `30s`). A healthy resource is re-checked on this cadence even without a spec change; this also bounds duplicate-identity loser-recovery latency (see [Scaling](#scaling)). |
| `--max-concurrent-reconciles`     | `1`     | Reconcile concurrency per kind. Values >1 **require `--leader-elect`** (refused at startup otherwise): concurrency relies on in-process locks that serialize shared cluster state, and a second active replica would race them — see [Scaling](#scaling) for what >1 actually parallelizes. |
| `--enable-webhooks`               | `false` | Serve the validating admission webhooks for `KafkaTopic`, `KafkaQuota`, `KafkaAccessPolicy`, `KafkaRoleBinding`, and `KafkaUser` (see [Admission webhook](#admission-webhook)). |

With `--enable-webhooks`, `/readyz` also gates on the webhook server having
started, so a pod is only marked Ready once it can actually serve admission
requests — important because the webhooks are installed with `failurePolicy:
Fail`, so a premature-Ready pod would black-hole CR create/update traffic
during rollout.

## Credentials via Kubernetes Secrets

In operator mode, secret references resolve from **Kubernetes Secrets** via
`valueFrom.secretKeyRef` — SASL username/password and TLS material (a custom CA
via `spec.tls.caCert`, client cert/key) alike. (The CLI resolves the same
references from `valueFrom.env` / `valueFrom.file` — see
[Connecting to Kafka](connecting.md).)

```yaml
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaCluster
metadata:
  name: prod
  namespace: monedula-system
spec:
  bootstrapServers: kafka-bootstrap.kafka.svc:9093
  tls:
    enabled: true
    # caCert:
    #   valueFrom:
    #     secretKeyRef:
    #       name: kafka-ca
    #       key: ca.crt
  auth:
    mechanism: SCRAM-SHA-512
    scram:
      username:
        valueFrom:
          secretKeyRef:
            name: kafka-admin-credentials
            key: username
      password:
        valueFrom:
          secretKeyRef:
            name: kafka-admin-credentials
            key: password
```

## Tenancy

`KafkaCluster.spec.tenancy` restricts which namespaces may reference a cluster
and which name prefixes those namespaces may manage:

```yaml
spec:
  tenancy:
    allowedNamespaces:        # glob patterns (path.Match); empty = any
      - team-payments
      - team-payments-*
    topicPrefixes:
      - namespaces: [team-payments, team-payments-*]
        prefixes:   [payments.]
```

Two levels of restriction apply. The **namespace allow-list**
(`allowedNamespaces`, when non-empty) applies to every data-plane kind. A
namespace is additionally **prefix-restricted** when it matches at least one
`topicPrefixes[].namespaces` glob: it may then only manage names starting with
one of that rule's prefixes (union across all matching rules). Consumer
**group names reuse the topic prefixes** by design — a team that owns the
`payments.` prefix owns `payments.`-prefixed consumer groups too. Resources
that cannot be scoped by a name prefix (the cluster resource, transactional
ids, delegation tokens, cluster-scoped roles) are denied outright for
prefix-restricted namespaces, because allowing them would let a tenant
escalate past its prefix (e.g. `Alter` on the cluster resource grants the
power to create arbitrary ACLs; a `SystemAdmin` role binding grants
everything).

What the operator enforces per kind:

| Kind | Namespace allow-list | Prefix-restricted namespaces additionally |
|------|---------------------|--------------------------------------------|
| `KafkaTopic` | yes | resolved `topicName` must match an allowed prefix; consumer **group** names in `spec.access` are prefix-checked too (they produce group ACLs / group role bindings) |
| `KafkaAccessPolicy` | yes | `topic` and `group` rule resources are prefix-checked (literal and `prefixed` pattern types alike); rules on `cluster`, `transactionalId`, or `delegationToken` resources are **denied** (unscopeable) |
| `KafkaRoleBinding` | yes | cluster-scoped bindings (empty `spec.resources`, e.g. `SystemAdmin`) are **denied**; each resource must be of type `Topic` or `Group` with a prefix-matching name; any other resource type is **denied** |
| `KafkaQuota` | yes | nothing — see limitations |
| `KafkaUser` | yes | nothing — a Kafka username has no prefix structure to check against, the same documented limitation as quota entities |

Enforcement is terminal: a denied resource gets `Phase: Error` with
`ValidationFailed=True`, reason `TenancyDenied`, and is not reconciled (no
live-state mutation, no requeue) until the spec, namespace, or tenancy config
changes. Namespaces that pass the allow-list but match no prefix rule are
unrestricted beyond the allow-list.

Limitations to know about:

- **Quota entities are not scoped.** A `KafkaQuota` targets a principal or
  client-id, which prefix rules cannot scope, so any allowed namespace can
  declare a quota for any principal — including the same entity another team
  already manages. Two `KafkaQuota`s resolving to the same entity on the same
  `KafkaCluster` are surfaced as a duplicate identity (the older wins; the
  newer goes terminal with `DuplicateIdentity`, see
  [Admission webhook](#admission-webhook)) rather than silently flapping — but
  the older claimant winning may still not be the team you intended. If that
  matters, restrict which namespaces may reference the cluster at all via
  `allowedNamespaces`.
- **`KafkaUser` usernames are not prefix-scoped**, for the same reason as quota
  entities: a Kafka username has no prefix structure to check a topic-prefix
  rule against. The namespace allow-list still applies (an unlisted namespace
  cannot create any `KafkaUser` at all); prefix-restricted namespaces get no
  additional restriction on which usernames they may declare.
- **Webhook enforcement is topic-only.** The admission webhook rejects tenancy
  violations at admission time for `KafkaTopic` only; for the other kinds a
  violation is admitted and then terminally rejected by the reconciler (the
  status carries `TenancyDenied`). Even for `KafkaTopic`, the tenancy check is
  skipped on an update to an object that already has a `deletionTimestamp`, so
  a tenancy tightening can never block deletion of a topic that already
  exists (see [Admission webhook](#admission-webhook)).
- **Tenancy assumes the `KafkaCluster` object (and its admin credentials) is
  controlled by the cluster owner.** A tenant who can create their own
  `KafkaCluster` pointing at the same brokers bypasses tenancy entirely. Run
  the operator with `--cluster-namespace` pointing at a namespace tenants
  cannot write to, so all `clusterRef`s resolve to centrally owned
  `KafkaCluster` objects.
- **Tenancy-denied resources still count toward the cluster-wide prune view.**
  A `KafkaTopic`/`KafkaAccessPolicy`/`KafkaRoleBinding` rejected with
  `TenancyDenied` keeps contributing its desired ACLs/role bindings to the
  cluster-wide co-ownership view that other resources' prune decisions are
  computed against — it is never itself reconciled, but its tuples are still
  protected from deletion. This is a deliberate fail-safe (retain rather than
  over-delete when tenancy flips transiently), not a bug.

The **CLI does not enforce** tenancy — it runs as the cluster admin and only
shape-validates the config; tenancy is a multi-team operator-mode control.

### Topology: one cluster, one `KafkaCluster`

One physical Kafka cluster should be represented by exactly **one**
`KafkaCluster` object. Running per-namespace `KafkaCluster` CRs that all point
at the same brokers — instead of one shared `KafkaCluster` referenced via
`--cluster-namespace` — recreates the cross-namespace prune flap the
cluster-wide ACL/role-binding views exist to prevent: each `KafkaCluster`
object gets its own view, so with `prune: true` two operators (or two
reconciles) can each see the other's tuples as undesired and fight over
deleting them. Use `--cluster-namespace` to point every tenant namespace's
`clusterRef` at one centrally owned `KafkaCluster` for multi-tenant setups.

## Admission webhook

**Division of labor — CRD validation rules vs. webhook.** Identity immutability
is the always-on floor: CEL rules (`x-kubernetes-validations`) baked into the
CRDs are enforced by the Kubernetes apiserver itself — no webhook, no certs,
no operator even running. They cover: `KafkaTopic.spec.topicName` (immutable
once set; an unset→set update is allowed since the name defaults from
`metadata.name`) and `spec.clusterRef.name`; `KafkaAccessPolicy.spec.clusterRef.name`;
`KafkaQuota.spec.entity`; the `KafkaRoleBinding` identity set
(`clusterRef.name`, `principal`, `role`, `scope.type`); and `KafkaUser.spec.username`
(immutable once set, with the same unset→set nuance as `topicName`) plus
`spec.clusterRef.name`. So even the default install (webhooks off) cannot
silently orphan broker state by renaming an identity field. The **webhook**
adds what CEL cannot express: *cross-object* checks — identity uniqueness,
tenancy, ACL Allow/Deny conflicts, and shape rules that need cluster context —
plus richer messages (e.g. resolved-name comparison for `topicName` and for
`KafkaUser.spec.username`).

**Identity uniqueness is enforced twice.** The webhook (when enabled) rejects a
duplicate broker identity *immediately, at admission*. Independently — webhook
on or off — the reconcilers for `KafkaTopic`, `KafkaQuota`, `KafkaRoleBinding`,
and `KafkaUser` detect duplicates *eventually-consistently*: before touching
any broker state, each reconcile scans the same-kind resources on the same
cluster (from the informer cache; contested verdicts are re-confirmed with an
uncached apiserver read — see [Scaling](#scaling)), and if another live
resource holds the same identity and is **older**
(earlier `creationTimestamp`; tiebreak: lexicographically smaller
namespace/name), the newer resource goes terminal — `ValidationFailed=True`
with reason `DuplicateIdentity` naming the older winner, `Ready=False`, phase
`Error`, a Warning event, and **no broker mutation**. The older resource keeps
reconciling normally. Resources being deleted never block others, and a
deleting loser still runs its finalizer — but the finalizer will not destroy
broker state another claimant still needs: when a deleting `KafkaUser` or
`KafkaQuota` finds another live (non-deleting) same-kind resource claiming the
same identity, its broker-side cleanup is **skipped** — the finalizer is
removed without touching the credential/quota, with an `OrphanedToCoClaimant`
event — and the surviving claimant keeps managing the identity. This holds
whichever side of the duplicate pair is deleted first (deleting the winner
while a loser waits also skips cleanup), so **deleting the losing duplicate is
the safe remediation**. (`KafkaRoleBinding` gets the same protection from the
cross-CR co-ownership shield; `KafkaTopic` deletion is protected by the
`Orphan` default and the allow-delete gate — and losers detected by this
operator version never gain a finalizer in the first place.)
When the older resource is deleted,
the loser recovers at its next periodic resync (≤ resync interval, default
`5m` — there is no same-kind cross-resource watch to trigger it sooner; see
[`--resync-interval`](#flags)). This replaces the old
webhook-off behavior where two resources claiming one identity silently
overwrote each other's broker state every resync (last-writer-wins flapping).
(`KafkaAccessPolicy` is excluded: a policy has no single broker identity; its
overlapping ACLs are handled by the cluster-wide co-ownership view and the
`ACLConflict` condition instead.)

With `--enable-webhooks`, the operator serves a validating admission webhook
covering five resources (disabled by default; cert-manager issues the serving
certificate):

- **`KafkaTopic`** — rejects a topic whose `(cluster, topicName)` identity is
  already owned by another `KafkaTopic` (uniqueness), a resolved `topicName`
  change or a `clusterRef` change (immutability), or a tenancy-policy
  violation. With the webhook disabled, the reconciler detects a duplicate
  identity eventually-consistently (the older topic wins; the newer goes
  terminal with `DuplicateIdentity` and never touches the broker) and applies
  tenancy terminally. Deletion
  is never blocked by tenancy: an update to a `KafkaTopic` that already has a
  `deletionTimestamp` (finalizer removal, an allow-delete annotation patch)
  skips the tenancy check, so tightening tenancy after a topic already exists
  can never make it undeletable.
- **`KafkaQuota`** — rejects invalid entity shape, duplicate `(cluster, entity)`
  identity, and entity or `clusterRef` immutability violations. With the
  webhook disabled, the reconciler enforces shape terminally and detects a
  duplicate identity eventually-consistently (older wins, newer goes terminal
  with `DuplicateIdentity`).
- **`KafkaAccessPolicy`** — rejects shape-invalid policies, a `clusterRef`
  change (immutability), and policies that introduce a cross-resource
  Allow/Deny conflict (a tuple granted `Allow` by
  another policy or a `KafkaTopic`'s topic-local access and `Deny` by the
  incoming policy, or vice versa). Whether or not the webhook is enabled, the
  reconciler sets a non-terminal `ACLConflict` status condition on every
  resource party to the conflict (the conflicting tuple is dropped from the
  applied union so reconciliation does not flap); the webhook adds fast
  admission rejection when enabled.
- **`KafkaRoleBinding`** (v0.14) — rejects invalid role-binding shape, duplicate
  `(cluster, principal, role, scope, resourcePattern)` identity, and immutability
  violations on `clusterRef`, `principal`, `role`, and `scope.type` (`resources`
  remains mutable). There is no cross-resource conflict check because RBAC is
  additive (no Deny). With the webhook disabled, the reconciler enforces shape
  terminally and detects overlapping compiled binding identities
  eventually-consistently (older wins, newer goes terminal with
  `DuplicateIdentity`).
- **`KafkaUser`** (v0.35) — rejects invalid password-source shape (not exactly
  one of `valueFrom`/`generate`; `inline`/`configMapKeyRef` password sources,
  which are never allowed), a duplicate `(cluster, username)` identity, and a
  `clusterRef` or resolved-username change (immutability — the resolved
  username is `spec.username`, falling back to `metadata.name`, closing the
  gap the CRD's CEL rule leaves around an unset→set update to a
  non-`metadata.name` value). With the webhook disabled, the reconciler
  detects a duplicate identity eventually-consistently (older wins, newer goes
  terminal with `DuplicateIdentity`). There is no tenancy check on this webhook
  (tenancy enforcement is topic-webhook-only by design). **Deletion is never
  blocked** — `KafkaUser`'s `ValidateDelete` always allows the request,
  because removing a CR can never violate an identity or shape invariant, and
  this is true for every resource kind's delete path in general — none of the
  five webhooks reject a delete on tenancy or identity grounds.

The webhook serving certificate is provisioned by
[cert-manager](https://cert-manager.io/) (apply the webhook manifests under
`config/` or use `--set webhook.enabled=true` with Helm).

## Reconciliation modes

`spec.reconciliation.mode` selects what the operator does with a detected diff —
identical to the CLI semantics:

| Mode          | Behavior                                                                 |
|---------------|-------------------------------------------------------------------------|
| `Enforce`     | (default) apply the diff, converging the cluster to the manifest.       |
| `DetectOnly`  | compute the diff and surface drift in status/metrics, but do not mutate.|
| `ObserveOnly` | observe live state only; report status without computing a drift verdict to act on. |

Reconcile concurrency defaults to **1 per controller**;
`--max-concurrent-reconciles` raises it (requires `--leader-elect` — see
[Scaling](#scaling) for what actually runs in parallel given the operator's
per-(cluster, substrate) locks, and why a single active replica is a hard
precondition).

The `KafkaTopic`, `KafkaAccessPolicy`, `KafkaQuota`, `KafkaRoleBinding`, and
`KafkaUser` controllers also watch `KafkaCluster` and reconcile their dependents promptly
when a referenced cluster appears or its spec changes — a `KafkaTopic` created
before its `KafkaCluster` no longer has to sit out the `ClusterNotFound` error
backoff (up to ~16m) or wait for the periodic resync (default `5m`), and
fixing a cluster's bootstrap servers/credentials, or adding
`authorization.mds`, promptly re-reconciles every dependent instead of
trickling in one resync at a time. Status-only `KafkaCluster` updates (the
cluster controller's own writes, every reconcile) are filtered out so they
don't storm every dependent. This also means adding `authorization.mds` to a
cluster can never leave `KafkaRoleBinding` reconciliation permanently wedged
in `MDSNotConfigured` — the role-binding controller's `MDSNotConfigured`
branch additionally requeues on the resync interval as a fallback.

## Scaling

**The consistency unit is (KafkaCluster, substrate) — not the CR kind.** The
operator writes broker-side access state through two *substrates*: **Kafka
ACLs** — written by the `KafkaTopic` and `KafkaAccessPolicy` reconcilers and
finalizers (prunes included) — and **Confluent MDS role bindings** — written
by the `KafkaRoleBinding` reconciler and finalizer, and by the `KafkaTopic`
reconciler on clusters whose `authorization.accessBackends` include `rbac`
(the topic-access→RBAC auto-map). Every such writer builds a cluster-wide
view (`ClusterACLView`, `ClusterRoleBindingView`): it lists every sibling CR
whose scope touches the same cluster (across kinds), aggregates their desired
tuples, reads live broker/MDS state, and applies the diff. That
read-compute-write span must not interleave with another writer's span on the
same substrate — two writers acting on independently built, possibly-stale
views race on prune and co-ownership decisions. Since v0.37 the operator
enforces exactly that with in-process keyed locks
(`internal/operator/locking`): all writers on one cluster's ACL substrate
serialize with each other, likewise all writers on its MDS substrate, while
different `KafkaCluster`s, the two substrates of one cluster, and
non-substrate work (`KafkaQuota`, `KafkaUser`, `KafkaCluster` health checks)
all run concurrently.

The substrate locks also closed a **latent cross-kind race** that existed
even at concurrency 1: the `KafkaTopic` and `KafkaAccessPolicy` controllers
(and, on the MDS side, the topic auto-map vs. the `KafkaRoleBinding`
controller) run in separate goroutines and each built their view
independently, so in rare interleavings one could prune a grant the other had
just created (healed at the next resync) — and, worst case, a stale
prune-consent snapshot could permanently delete an out-of-band ACL despite a
live `prune: false` veto. v0.37 makes each view-build→apply span atomic per
(cluster, substrate), so those interleavings can no longer occur.

**`--max-concurrent-reconciles` (default `1`) sets each controller's worker
count, and values >1 are honored as of v0.37** (earlier versions clamped them
to 1). Given the locks, what `>1` actually parallelizes is: reconciles
touching **different clusters**, the **two substrates** of one cluster, and
the **non-substrate kinds** (`KafkaQuota`, `KafkaUser`, `KafkaCluster`).
Writers on the *same* cluster's *same* substrate still execute one at a time
regardless of the setting — so raising it helps multi-cluster installs and
quota/user-heavy workloads, but does **not** raise the throughput of, say,
hundreds of `KafkaTopic`s sharing one cluster. **`>1` requires
`--leader-elect`**: the serialization above is *in-process* locking, so it
cannot protect against a second **active replica** racing the first
cross-process — leader election guarantees a single active replica, which is
what makes the in-process locks sufficient. The operator refuses to start
with `>1` and no `--leader-elect` (exit code 2), and the Helm chart mirrors
the guard at template time (`maxConcurrentReconciles > 1` with
`leaderElection.enabled=false` fails `helm template`/`install`). Possible
future scale work: a per-cluster view actor maintaining a shared,
incrementally-updated managed-set/view (instead of each reconcile rebuilding
it from a List) could raise *same-cluster* throughput; the locks are the
shipped mechanism, and such an actor would only widen concurrency within one
(cluster, substrate), not change the correctness model.

**The duplicate-identity gate under concurrency.** Two same-kind reconciles
claiming the same broker identity serialize on a per-(cluster, kind,
identity) lock (always taken before any substrate lock), so the gate's
older-wins verdict is never computed twice in parallel — and the same lock
covers the `KafkaUser`/`KafkaQuota` deletion co-claimant scans. On
*contested* paths the gate additionally re-checks its cached verdict against
the apiserver directly (an uncached quorum `List`, bypassing the informer
cache): when the cached scan finds a rival, and when a CR that has never been
`Ready=True` finds none (the young-CR cache-lag window); deletion co-claimant
scans re-check only when they are about to destroy broker state. Every error
direction fails safe — requeue rather than mutate, and never destroy state a
quorum-revealed co-claimant still needs. The steady-state hot path (CR
`Ready=True`, no cached rival) makes **zero** apiserver round-trips for the
gate.

**The resync cadence (`--resync-interval`, default `5m`, minimum `30s`) is
the main lever you have.** It governs: how quickly out-of-band drift (a
manual broker change) is re-detected without a spec change; how quickly a
duplicate-identity loser recovers once the winning CR is deleted (there is no
same-kind cross-resource watch for that case); and the `MDSNotConfigured`
fallback re-check for `KafkaRoleBinding`. Lowering it trades faster
convergence for more load — see below.

**What to expect at hundreds of CRs.** Every reconcile of a `KafkaTopic`,
`KafkaAccessPolicy`, `KafkaQuota`, or `KafkaRoleBinding` pays two costs beyond
its own object: a `List` of same-kind sibling CRs (for the duplicate-identity
gate and the cluster-wide view) — served from the manager's informer cache, so
it is an in-process scan whose CPU cost grows with the CR count per kind, not
an apiserver round-trip — and, for ACL-backed clusters, a `ListACLs` call
against the broker to observe live state, which **is** an uncached network
call re-issued on every reconcile. At small counts (tens of CRs) this is
unnoticeable; at hundreds of CRs sharing one `KafkaCluster`, expect `ListACLs`
to grow with the broker's total ACL count — paid on every reconcile of every
CR of that kind, including every
periodic resync. One more cost applies only to unhealthy resources: a CR that
is not currently `Ready=True` takes the quorum-recheck path of the
duplicate-identity gate (see above) and pays **one uncached apiserver `List`
of its same-kind siblings per reconcile attempt**. This is expected and
bounded — fleet-wide during a broker outage (every CR goes non-Ready and
retries on error backoff), or persistently for `DetectOnly` CRs that sit in
drift — by the backoff/resync cadence, and it self-clears as soon as CRs
return to `Ready=True`, which restores the zero-round-trip hot path. If
reconcile latency or broker load becomes a concern at scale,
`--resync-interval` is the primary knob (a longer interval trades slower
out-of-band drift detection for less steady-state load);
`--max-concurrent-reconciles` is not, when the CRs share one cluster — its
same-substrate writers serialize on the per-cluster locks regardless (see
above).

**Per-cluster snapshot caching was deliberately not implemented.** An obvious
optimization would be to cache each `KafkaCluster`'s live ACL/quota/role-binding
state across reconciles instead of re-listing it every time. This was
intentionally left out: a cached snapshot can go stale between a broker-side
change and the next cache refresh, and reconciling against stale live state
risks exactly the kind of incorrect diff/prune decisions the duplicate-identity
gate and cluster-wide views are designed to avoid. Every reconcile reading live
state fresh is slower but honest — what you see in `status` reflects what the
broker actually has at that moment, not what the operator last cached. This
trade-off is revisited if per-reconcile List cost becomes a proven bottleneck
in practice.

## Risk gates via annotations

Risky operations are gated exactly as in `apply`, but in the operator the gates
are opened by **annotations on the resource** rather than CLI flags:

| Annotation                              | Permits                                                     |
|-----------------------------------------|------------------------------------------------------------|
| `gitops.monedula.dev/allow-destructive` | Partition increase and other destructive ops.              |
| `gitops.monedula.dev/allow-delete`      | Deletion of the managed Kafka objects (topic / ACLs).      |

Without the matching annotation, those operations are reported as **Blocked** in
the resource's status and are not executed; the rest of the diff still applies.

ACL pruning is gated **declaratively** instead: set `spec.prune: true` on the
`KafkaTopic` / `KafkaAccessPolicy` (default `false`). Without it, in-scope ACLs
that are no longer desired are reported as drift (`PruneDisabled`) but never
deleted; a prune executes only when every resource whose scope covers the ACL
has opted in.

## Finalizers and teardown

The operator places the finalizer `gitops.monedula.dev/finalizer` on resources
it manages so it can run cluster-side cleanup before the object is removed.
`spec.deletionPolicy` controls what teardown does. The default varies by kind
— see [Deletion policies](manifest-reference.md#deletion-policies) for the
full table — but the two values always mean:

- **`Orphan`**: leave the managed Kafka objects (topics / ACLs / MDS role
  bindings) in place when the resource is deleted.
- **`Delete`**: delete the managed Kafka objects — gated behind the
  `gitops.monedula.dev/allow-delete: "true"` annotation for `KafkaTopic` and
  `KafkaAccessPolicy`. Without it, the finalizer is retained (a Warning event
  is emitted) so nothing is silently destroyed. `KafkaUser` and
  `KafkaRoleBinding` are ungated exceptions — see below.

If the backing cluster is unreachable and cleanup cannot complete, the
finalizer would otherwise block deletion forever. The escape hatch
`gitops.monedula.dev/force-finalizer-removal: "true"` removes the finalizer
**without** cluster-side cleanup so the object can be garbage-collected.

**`KafkaUser` and `KafkaRoleBinding` are exceptions to the `allow-delete`
gate.** Under `deletionPolicy: Delete` (the default for both kinds), their
finalizers remove the managed external state **without** requiring the
`allow-delete` annotation — deleting the CR is itself the explicit action,
since that state is each resource's entire reason to exist:

- **`KafkaUser`**: removes the declared mechanism's SCRAM credential. A
  deletion failure emits a `CredentialDeletionFailed` Warning event but does
  not block finalizer removal (best-effort).
- **`KafkaRoleBinding`**: removes the compiled MDS role binding(s) (subject to
  the co-ownership shield — bindings still desired by another live CR are
  retained). Unlike `KafkaUser`, a removal error here **retains** the
  finalizer and requeues with backoff until cleanup succeeds, since an
  incomplete MDS cleanup would otherwise leave orphaned bindings.

## Generated credentials (`KafkaUser` generate mode)

With `spec.password.generate: {}`, the operator mints the password itself
(32 random bytes, base64url-encoded) instead of reading it from a Secret you
manage. It creates and owns a Secret named **`<KafkaUser name>-kafka-credentials`**
holding `username`, `password`, and `mechanism` keys, labelled
`gitops.monedula.dev/credential-source: "true"` and carrying an owner
reference to the `KafkaUser` — Kubernetes garbage-collects it automatically on
CR deletion; the operator's RBAC deliberately excludes `delete` on Secrets, so
it never removes this Secret itself. The Secret is always created **before**
the SCRAM upsert call, so a crash between the two steps leaves a recoverable
stored-but-not-yet-applied password rather than an unreadable broker-only
credential.

**Rotation by deletion.** The generated password is regenerated only when you
delete that Secret — this is the explicit, user-initiated rotation signal.
Editing the `KafkaUser` spec itself never regenerates it. A pre-existing
Secret at the generated name that is **not** owned by this `KafkaUser` is
never adopted or overwritten: the reconciler reports a terminal
`ForeignSecret` error naming it instead, so it never silently takes over
(or clobbers) a Secret that already means something else.

## Status, conditions, and events

Each resource carries a `status` with a `phase` and a set of `conditions`
(e.g. `SchemaSynced`, `ACLConflict`, `SchemaRegistryDegraded` — set on a
`KafkaTopic` when the Schema Registry's global-compatibility read fails
during a reconcile, so a first-time subject compatibility set falls back to
the legacy classification instead of silently degrading with no signal;
`UserSynced` — set on a `KafkaUser` after SCRAM credential operations are
applied: `True` when the declared credential converges, `False` when one or
more operations could not be applied); the
reconcilers also emit Kubernetes Events (Normal/Warning) for notable
transitions such as blocked operations or forced finalizer removal. The
`SharedACLsRetained`/`SharedRoleBindingsRetained` events (emitted when a
deletion's co-ownership shield retains tuples another live CR still needs,
see [Finalizers and teardown](#finalizers-and-teardown)) name up to three
co-owning resources (`Kind/namespace/name`), with an "and N more" overflow
beyond that. The operator exposes Prometheus metrics on
`--metrics-bind-address`:

- `monedula_reconcile_total` — reconcile invocations, by controller and result.
- `monedula_reconcile_errors_total` — reconcile invocations that errored, by controller.
- `monedula_reconcile_duration_seconds` — reconcile duration histogram, by controller.
- `monedula_kafka_cluster_reachable` — 1/0 cluster reachability, by namespace and name.
- `monedula_kafka_topic_drift_detected` — 1/0 topic drift, by namespace and name.
- `monedula_managed_topics` — number of `KafkaTopic`s observed by the operator.
- `monedula_access_policy_drift_detected` — 1/0 access-policy drift, by namespace and name.
- `monedula_kafka_quota_drift_detected` — 1/0 quota drift, by namespace and name.
- `monedula_managed_quotas` — number of `KafkaQuota`s observed by the operator.
- `monedula_kafka_rolebinding_drift_detected` — 1/0 role-binding drift, by namespace and name.
- `monedula_managed_rolebindings` — number of `KafkaRoleBinding`s observed by the operator.
- `monedula_kafka_user_drift_detected` — 1/0 `KafkaUser` identity (username/mechanism/iterations) drift, by namespace and name. Passwords are never part of this signal — see [Watching credential Secrets](#watching-credential-secrets).
- `monedula_managed_users` — number of `KafkaUser`s observed by the operator.
- `monedula_reconcile_terminal_total` — terminal reconcile outcomes (no retry without a human/spec change — `ValidationFailed`, `TenancyDenied`, `DuplicateIdentity`, `MDSNotConfigured`, ...), by kind and reason. No per-CR labels (kind+reason only), so cardinality stays bounded regardless of CR churn; useful for alerting on a resource class stuck needing manual intervention.

With `--metrics-secure`, the endpoint is served over HTTPS and requires a
bearer token authorized (via `SubjectAccessReview`) for the non-resource URL
`/metrics` — set `metrics.serviceMonitor.scheme`/`bearerTokenSecret`/`tlsConfig`
in the Helm chart (or the equivalent Prometheus scrape config) to match.

## Limitations

- **File-based schema references are unsupported in operator mode.** A
  `KafkaTopic` whose `spec.schema` body comes from a `valueFrom.file` (or `env`)
  cannot be resolved in-cluster: the controller sets `SchemaSynced=False` with
  reason `SchemaUnresolved` and still reconciles the topic and access. Supply
  schema bodies via `secretKeyRef`, `valueFrom.inline`, or
  `valueFrom.configMapKeyRef`. For prompt reconcile on ConfigMap body changes,
  add the `gitops.monedula.dev/schema-source: "true"` label to the ConfigMap;
  without it, changes apply at the next periodic resync and the topic reports
  `SchemaSourceUnwatched`.

## Watching credential Secrets

The operator watches label-selected credential Secrets and promptly
re-reconciles the dependent resources when those Secrets change. A Secret that
carries the `gitops.monedula.dev/credential-source: "true"` label opts into this
prompt watch: when it rotates, the operator re-reconciles the referencing
`KafkaCluster` and its data-plane resources (topics, policies, quotas, role
bindings) immediately, rather than waiting for the periodic resync.

**`KafkaUser` password Secrets (`valueFrom.secretKeyRef`) follow the same
rule.** Label the referenced Secret with `gitops.monedula.dev/credential-source:
"true"` so a password change is picked up promptly: the operator compares the
Secret's `resourceVersion` against `status.appliedPasswordRef` on every
reconcile and re-upserts the SCRAM credential when it has changed. Without the
label, the rotation is still picked up — but only at the next periodic resync
(≤ resync interval, default `5m` — see [`--resync-interval`](#flags)), not
immediately. (Secrets the operator itself creates in
`generate` mode are labelled automatically and always watched via their owner
reference — this note applies only to Secrets *you* manage and reference via
`secretKeyRef`.)

```yaml
# A KafkaCluster whose SCRAM credentials come from the labelled Secret in
# secret.yaml. Because that Secret carries gitops.monedula.dev/credential-source,
# the operator re-reconciles this cluster (and its topics/policies/quotas/role
# bindings) promptly when the Secret rotates, rather than waiting for the resync.
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaCluster
metadata:
  name: prod
spec:
  bootstrapServers: kafka.prod.svc:9092
  auth:
    mechanism: SCRAM-SHA-512
    scram:
      username:
        valueFrom:
          secretKeyRef:
            name: kafka-creds
            key: username
      password:
        valueFrom:
          secretKeyRef:
            name: kafka-creds
            key: password
```

```yaml
# A credential Secret opted into the operator's prompt watch: the
# gitops.monedula.dev/credential-source label makes the operator re-reconcile
# the referencing KafkaCluster and its data-plane resources promptly when this
# Secret rotates, instead of waiting for the periodic resync.
apiVersion: v1
kind: Secret
metadata:
  name: kafka-creds
  labels:
    gitops.monedula.dev/credential-source: "true"
type: Opaque
stringData:
  username: svc-monedula
  password: change-me
```
