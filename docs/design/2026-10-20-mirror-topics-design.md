# Design: declarative mirror topics (Confluent Cluster Linking)

Original proposal: 2026-10-20  
Revision: 2026-10-21  
Status: proposed — v7 (v6 + repository alignment notes for the finalizer contract)  
Repository: https://github.com/monedula-dev/monedula-gitops

## Summary of changes in v7

v6 is accepted as the normative finalizer contract. v7 only records how it maps onto code that
already exists, so the implementation plan does not build mechanisms twice:

1. **Co-claimant scan for `KafkaTopic` is new, but the pattern is not.** Today only the `KafkaUser`
   and `KafkaQuota` finalizers run a deletion co-claimant scan (`findLiveUserCoClaimant`,
   `findLiveQuotaCoClaimant` in `internal/operator/controller/duplicate.go`); `KafkaTopic` relies on
   `allow-delete` plus the `Orphan` default. The v6 "full orphan" branch is the same age-blind
   remove-finalizer-without-broker-cleanup rule those two kinds use, extended to `KafkaTopic` and
   to cross-kind claimants. The existing ACL co-ownership shield (`shieldACLDeletion`,
   `internal/operator/controller/aclview.go`) stays in place for the no-claimant path; full orphan
   is stricter than the shield, not a replacement for it.
2. **Placement of the scan.** In today's `KafkaTopic` reconcile the destination clients are built
   before the deletion handler, and an unreachable cluster with policy `Delete` retains the
   finalizer (`handleUnreachableDeletion`) unless force-removal is set. v6 requires the
   co-claimant scan and the `Orphan`/force exits to run **before** client construction, so that
   branch is reached even when the cluster is unreachable. This is a reorder of the deletion
   branch, not a change to `handleUnreachableDeletion` itself.
3. **`Absent` recovery needs no new mechanism.** The franz `DeleteTopic` already treats
   `UNKNOWN_TOPIC_OR_PARTITION` as success (`internal/kafka/franz/client.go`, and the mock mirrors
   it), so a restart after a successful delete is idempotent today. v6's "skip `DeleteTopic` on
   confirmed `Absent`" is satisfied by that behavior plus the shared classifier; the new
   requirement is only that `Absent` must come from the classifier (Kafka absent **and** no
   contradictory REST mirror entry), never from a remembered delete.
4. **No reconcile deadline exists.** The operator does not set a per-reconcile context deadline, so
   "the earlier of the budget and the remaining reconcile deadline" reduces to the
   `finalizerVerificationTimeout` budget unless the reconcile context already carries a deadline.
   The wording stands; no deadline plumbing is implied.

## Summary of changes in v6

This revision applies the v5 review to the `KafkaTopic` deletion finalizer. The functional
baseline remains v5; the mirror API, shared classification, claim identity, import policy, and
orphan-only `KafkaMirrorTopic` lifecycle are otherwise retained.

1. **An authoritative verification attempt, not a single describe.** The final check combines a
   fresh Kafka Admin read of the exact destination topic with a fresh, fully paginated
   `GET /v3/clusters/{cluster_id}/links/-/mirrors` pass. A known-link describe cannot prove that a
   topic is not mirrored on another link. Incomplete evidence means `Unknown`, never permission
   to delete. No undocumented wildcard describe or name-filter shortcut is assumed.
2. **Explicit `Absent` recovery.** Confirmed absence completes the topic-deletion step without
   another `DeleteTopic`. Remaining ACL/schema cleanup is still subject to approval and ownership
   checks. A restart after successful deletion must not require the topic to reappear or trap the
   CR's finalizer solely because the topic is absent.
3. **Full orphan wins before REST checks.** A positively identified active co-claimant causes the
   finalizer to skip all remaining topic, ACL, and schema cleanup, emit a warning, and remove its
   finalizer. This decision is made before external cluster reads and applies even if the topic is
   a live mirror or REST is unavailable. A failed claim scan is not a negative result.
4. **Honest, bounded lock behavior.** Full preparation stays outside the ACL substrate lock. The
   final verification may perform multiple page requests under that lock, but has one total,
   non-renewable deadline with no retries or backoff. Other ACL writers may wait during it; the
   added verification delay is bounded, not zero. The finalizer still takes no identity lock.

V6 explicitly chooses **full orphan**, rather than selective ACL cleanup, for the positive
co-claimant branch. It also proposes a **5-second default** for the operator-level final
verification budget; this is a configurable safety default, not a measured performance claim.
Acceptance tests and release gates below cover these decisions and partial-cleanup outcomes.

The current finalizer section is normative. Historical revision summaries describe earlier
intent; they do not override v6's decision precedence, verification algorithm, or timeout rules.
This document remains a design proposal, not evidence that repository changes or integration
tests have already been implemented.

## Summary of changes in v5 (historical)

V5 introduced two corrections to the v4 `KafkaTopic` finalizer guard:

1. **Co-claimant at deletion: skip and orphan, do not defer.** It adopted the existing
   leak-never-destroy pattern for a competing claim rather than waiting for that claim to
   disappear. V6 makes this a full topic/ACL/schema orphan and gives it precedence over external
   state reads. Orphaning does not itself remove a normal topic or make mirror creation possible.
2. **Preserve the finalizer's lock discipline.** It retained the ACL substrate lock without adding
   an identity lock. Its original single-describe assumption and zero-stall wording were
   insufficient; v6 replaces them with the explicit, bounded verification contract below.

## Summary of changes in v4 (historical)

Four additional integration safety corrections, without changing the separate-kind or REST v3
approach:

1. **Canonical claim identity.** CLI and operator claims use an effective destination topic name
   plus a *resolved* KafkaCluster reference. Operator namespace-local references resolve to
   `<resource namespace>/<clusterRef.name>`; with the `ClusterNamespace` override, they resolve
   to `<ClusterNamespace>/<clusterRef.name>`. Same-named KafkaCluster objects in different
   namespaces are distinct claims when references are namespace-local. Both resource kinds use
   exactly the same resolver, scan scope, duplicate comparator, and per-identity lock.
2. **REST/Admin cluster binding.** Before constructing a usable mixed Kafka Admin + REST snapshot
   or performing a REST mutation, compare the destination Kafka cluster ID from the Kafka protocol
   with the REST target's effective `clusterId`. A missing or different ID is a
   `ClusterIdentityMismatch`/`StateReadError`, not permission to mix inventories from two clusters.
3. **KafkaTopic deletion-path mirror guard.** An existing `KafkaTopic` with
   `deletionPolicy: Delete` must classify live topic state *before any destructive cleanup* in
   its finalizer, not just during ordinary diff/reconcile. A mirror, stopping mirror, unreadable
   classification or active cross-kind claimant blocks cleanup; finalizer remains pending unless
   a deliberate orphan-only escape hatch is used. Recheck as close to deletion as possible and
   document the unavoidable REST/Kafka TOCTOU window. The pending-finalizer behavior for a
   positive co-claimant was superseded in v5/v6 by full orphan completion.
4. **Effective-name immutability at the schema boundary.** `KafkaMirrorTopic` must enforce
   immutability of the resolved `topicName` with spec-scoped CEL that permits unset-to-explicit
   only when the effective destination name stays identical. The webhook and CLI check the same
   invariant; the schema must remain safe when webhooks are disabled.

Smaller integration clarifications: REST create payload shape (`configs` as `{name, value}`
entries), just-in-time state rechecks before writes, `STOPPED` import with competing claims,
operator Secret rotation, and tests for namespace-local/global cluster resolution and finalizer
failure paths.

## Summary of changes in v3

Two corrections against the repository's existing behavior; the conceptual contract of v2 is
unchanged:

1. **Duplicate-claim tie-break.** v2 blocked both resources when creation timestamps tie. The
   repository already has a strict total order (`olderIdentityClaim` in
   `internal/operator/controller/duplicate.go`: earlier `creationTimestamp`, then lexicographically
   smaller `namespace/name`). v3 keeps that order and extends it with the resource kind as a final
   key, because a `KafkaTopic` and a `KafkaMirrorTopic` can share the same `namespace/name`. Exactly
   one claimant always wins; the others are blocked.
2. **Rejecting `deletionPolicy: Delete`.** The CLI already rejects unknown fields (strict decode in
   `internal/loader`), but in the operator the API server silently prunes fields absent from the
   CRD schema, so a webhook would never see them. v3 therefore keeps `deletionPolicy` in the schema
   with an enum restricted to `Orphan`.

## Summary of changes from v1 (in v2)

This revision retains a separate `KafkaMirrorTopic` kind and the Confluent Kafka REST v3 transport,
but makes the safety and state-management contracts explicit:

1. A **shared, typed topic-classification snapshot** feeds CLI diff/apply/verify, import, the
   `KafkaTopic` mirror guard, and operator reconciliation. Transport errors are not equivalent to
   absent topics or an empty mirror list.
2. If Kafka REST is configured but its relevant reads fail or are incomplete, affected mutations
   **fail closed**. List responses must be fully paginated.
3. Link prefix validation explicitly reads link configurations; a missing/unknown prefix is not
   assumed to be empty.
4. `STOPPED` and `PENDING_STOPPED` have distinct handling. `STOPPED` remains visible in the REST
   mirrors list but is now a normal topic; `PENDING_STOPPED` is protected during transition.
5. Lifecycle-only drift has a first-class, **non-executable diagnostic** representation, separate
   from executable operations and hard rejections. `verify` does not require a config diff to detect
   an unhealthy mirror.
6. Mirror deletion by the operator is **out of scope for v1**. Deleting a CR always orphans the
   Kafka topic; no time-of-check/time-of-use race can cause deletion of a promoted topic.
7. `replicationFactor` is **create-time only**. A mismatch on an existing mirror is detected but
   never automatically reconciled.
8. Import is conservative about mirrored topic configurations and ACLs. When ownership cannot be
   inferred, it emits diagnostics instead of silently adopting external state.
9. The identity space, failure semantics, and high-risk edge cases have explicit CLI/operator/e2e
   acceptance criteria.

## Problem

Teams using Confluent Cluster Linking today have two ways to get mirror topics onto a destination
cluster:

1. **Auto-create mirror topics** on the link (`auto.create.mirror.topics.enable=true`). This is
   convenient, but the link principal needs access on source topics and mirrors appear as a side
   effect: Git does not say which topics should be mirrored, and there is no per-topic review.
2. **Create mirrors by hand** with the Confluent CLI (`confluent kafka mirror create`) or
   `kafka-mirrors`. These actions are imperative. A mirror that goes `FAILED` or `PAUSED` is not
   necessarily reported by the existing GitOps drift/verify workflow.

monedula-gitops already manages topics, ACLs, quotas, users, and role bindings declaratively.
Mirror topics should follow the same review-in-Git, diff, import, and drift-reporting workflow.

Cluster links are relatively rare, long-lived, and contain cross-cluster credentials. This design
manages **mirror topics on pre-existing destination cluster links**, not the links themselves.

## Goals

1. Introduce `KafkaMirrorTopic` to declare a destination mirror of a source topic on an existing
   cluster link.
2. Support both delivery modes: CLI (`validate`, `diff`, `apply`, `import`, `doctor`, `verify`) and
   Kubernetes operator (CRD, controller, webhook, status, metrics, Helm/RBAC).
3. Detect a missing mirror, incorrect link/source identity, declared local config drift,
   replication-factor mismatch, and non-healthy or promoted lifecycle state.
4. Import existing mirrors without importing the same physical topic as two resource kinds.
5. Never require source-cluster credentials or permissions beyond those used by the pre-existing
   cluster link. In particular, no auto-create requirement.
6. Fail safely when Kafka REST state is unavailable, incomplete, or inconsistent with Kafka Admin.
7. Preserve a single destination-topic identity space shared by `KafkaTopic` and
   `KafkaMirrorTopic`.

## Non-goals

- Creating, modifying, pausing, or deleting **cluster links**. The tool may only read link state
  and configuration to validate a mirror declaration.
- Running mirror lifecycle operations: `pause`, `resume`, `promote`, `failover`, `restore`,
  `reverse-and-start`, or similar operational controls. These stay in Confluent tooling.
- Deleting a mirror as part of manifest removal, CR deletion, or drift reconciliation **in v1**.
- Managing access inside `KafkaMirrorTopic`; use `KafkaAccessPolicy` or `KafkaRoleBinding`.
- Managing Schema Linking or schema replication.
- Supporting MirrorMaker 2 or Replicator.
- Automatically changing replication factor on an existing mirror.
- Inferring which source-synced configs or ACLs should become locally GitOps-managed if their
  provenance cannot be verified.

## Safety invariants

1. Reconciliation must never promote, fail over, reverse, restore, or otherwise convert a mirror
   into a writable normal topic.
2. It must never delete an existing normal topic in order to satisfy `KafkaMirrorTopic`.
3. It must never edit a live mirror through `KafkaTopic` when mirror classification is available.
4. It must never treat a failed, unauthorized, malformed, or partially paginated REST response as
   proof that a mirror does not exist.
5. It must never create a mirror when the destination topic already exists, regardless of kind.
6. A destination `(resolved KafkaCluster, effective topicName)` may be claimed by **at most one**
   of `KafkaTopic` or `KafkaMirrorTopic`, within or across namespaces referencing that cluster.
   Resolving a cluster includes namespace-local versus operator-global cluster namespace rules.
7. Destructive mirror deletion is unavailable in v1, including via operator finalizers.
8. An observed lifecycle transition must not cause an implicit ownership transfer between kinds.
9. When REST is configured, a `KafkaTopic` deletion finalizer must not **knowingly** delete a
   live mirror, stopping mirror, or object whose classification is unknown; it must classify
   before topic, ACL, or schema cleanup. A read failure never authorizes cleanup.
10. Kafka Admin and Confluent REST must identify the **same destination Kafka cluster** before
    their observations are merged or REST writes are allowed. A mismatch is a hard safety error.
11. Identity checks and regular-reconcile serialization between `KafkaTopic` and
    `KafkaMirrorTopic` use the same canonical claim key; a kind-qualified lock would not
    prevent cross-kind mutation races. The deletion finalizer remains the explicit exception
    to identity locking and uses the co-claimant/verification gates specified below.
12. A positive deletion co-claimant result permits only full orphan completion, not selective
    topic, ACL, or schema cleanup. This branch needs no Kafka/REST state and must not be held
    pending by a REST error; failed Kubernetes claim reads still block a destructive path.
13. Confirmed `Absent` is a completed topic-deletion step, not an error. It never bypasses
    approval or ownership checks for remaining cleanup and is never inferred from a timeout.

The guarantees above are safety decisions made on the last observed state. They are not an
atomic compare-and-delete protocol: if an external actor changes a topic between the final
classification and a legacy Kafka `DeleteTopic` request, the tool cannot eliminate that
TOCTOU race. The default `KafkaTopic` deletion policy remains `Orphan`, and mirror deletion is
still entirely disabled in v1.

## Connectivity and API ownership

Mirror creation and mirror identity/lifecycle data require **Confluent Kafka REST v3**. They cannot
be implemented solely with the standard Kafka protocol via franz-go. On Confluent Platform this
API is exposed by Confluent Server; on Confluent Cloud it is available through the cluster's Kafka
REST endpoint.

`KafkaClusterSpec` gains an optional `rest` block:

```yaml
spec:
  rest:
    endpoint: https://broker-0.dest:8090/kafka # base; client appends /v3/...
    auth:                                     # same shape as authorization.mds.auth
      type: basic
      username: { valueFrom: { env: DEST_REST_USER } }
      password: { valueFrom: { secretKeyRef: { name: dest-rest, key: password } } }
    tls: { ... }                              # existing TLSConfig shape
    clusterId: lkc-abc123                    # optional on Platform; required on Cloud
```

The `endpoint` is a **base URL without `/v3`**. The client joins paths without introducing a
second `/kafka`, duplicate slash, or ambiguous escaping; topic and link names are URL path-escaped.

When `clusterId` is empty, the client attempts `GET /v3/clusters` and requires a uniquely
resolvable destination cluster ID. Confluent Cloud users must provide `clusterId`. An unsuccessful
or ambiguous discovery is an error, not a best-effort guess. Resolution may be cached for the
lifetime of a run/reconcile and invalidated on errors.

### Destination Kafka cluster ID binding (required)

The Kafka Admin connection (`bootstrapServers`) and Confluent REST (`rest.endpoint` / REST
`clusterId`) are independent endpoints. Their configuration alone does not prove that they reach
the same Kafka cluster. The runtime must obtain the authoritative **destination Kafka cluster ID**
from the standard Kafka Admin/metadata protocol and compare it to the effective Kafka REST
cluster ID (configured or discovered):

- Require both IDs to be non-empty, usable, and **equal**. Use exact Kafka cluster identifiers;
  do not compare bootstrap hostnames, human-readable aliases, or cluster CR names.
- Compare IDs after REST discovery (if needed), **before** accepting a combined live-state
  snapshot, mirror create/config alter, or `KafkaTopic` mirror-sensitive destructive cleanup.
  The same check applies to Confluent Platform and Confluent Cloud.
- An inaccessible Kafka ID, unreadable REST cluster identity, or mismatch produces
  `ClusterIdentityMismatch` (an operational `StateReadError` finding) and blocks affected writes.
  Do not combine Kafka inventory from cluster A with link/mirror data from cluster B.
- If the existing Kafka Admin abstraction does not yet expose cluster ID, extend its interface
  and mock; do not skip this invariant on any platform. For a configured REST `clusterId`, still
  validate it against Kafka Admin rather than trusting the configuration.
- Resolve and verify at least once per CLI run or operator reconcile before consuming REST data;
  on client reconstruction, changed cluster config/credentials, or an identity-relevant error,
  invalidate the cached comparison. `doctor` reports both IDs and their match result.

Two different `KafkaCluster` CRs may intentionally point to the same physical Kafka cluster.
Within Kubernetes, claim identity is scoped to the **resolved KafkaCluster object**, not global
physical cluster ID, to preserve existing operator ownership semantics. Such aliased CRs are not
automatically deduplicated against one another; `doctor` should warn if it can prove such aliasing.

Clusters without `rest` cannot host `KafkaMirrorTopic`: CLI `validate` rejects the reference and
the operator reports `RESTNotConfigured`. `KafkaTopic` on a cluster without `rest` retains its
legacy behavior, but cannot be guaranteed to detect an out-of-band mirror; `doctor` should warn
about that limitation rather than claiming Kafka rejects every possible mutation.

### REST endpoints

Paths below are relative to the configured `/kafka` base URL:

| Purpose | API |
|---|---|
| Discover cluster ID (Platform) | `GET /v3/clusters` |
| List destination links | `GET /v3/clusters/{cluster_id}/links` |
| Describe a destination link | `GET /v3/clusters/{cluster_id}/links/{link}` |
| Read link configs, including `cluster.link.prefix`, `topic.config.sync.include`, `acl.sync.enable`, `auto.create.mirror.topics.enable` | `GET /v3/clusters/{cluster_id}/links/{link}/configs` |
| Read a particular link config, if useful | `GET /v3/clusters/{cluster_id}/links/{link}/configs/{config_name}` |
| List all destination mirrors, across links | `GET /v3/clusters/{cluster_id}/links/-/mirrors` |
| Describe a mirror | `GET /v3/clusters/{cluster_id}/links/{link}/mirrors/{mirror}` |
| Create a mirror | `POST /v3/clusters/{cluster_id}/links/{link}/mirrors` (`source_topic_name`, optional `mirror_topic_name`, `replication_factor`, `configs`) |
| Alter a destination topic config | `POST /v3/clusters/{cluster_id}/topics/{topic}/configs:alter` |

The create request encodes `configs` using the REST v3 API's list-of-entries representation,
**not** the manifest's YAML string map. For example (illustrative payload; omit optional fields
when not specified):

```json
{
  "source_topic_name": "payments.orders",
  "mirror_topic_name": "payments.orders",
  "replication_factor": 3,
  "configs": [{"name": "min.insync.replicas", "value": "2"}]
}
```

The client must map each declared config key to one `{ "name": ..., "value": ... }` entry and
use the tested version-specific wire schema; avoid blindly serializing `spec.config`. The
unprefixed-link case may omit `mirror_topic_name` according to the endpoint's contract.

**Deliberately absent:** `DELETE /v3/clusters/{cluster_id}/topics/{topic}`. Although available in
Confluent REST, v1 does not call it for mirrors.

Standard Kafka Admin (the existing client) remains the preferred source for destination-topic
existence, replication factor, and topic configs. REST is authoritative for cluster-link identity,
mirror lifecycle/identity, and link configuration. We do not need to implement a parallel REST
client for all Kafka topic administration.

Client structure: `internal/kafkarest` interface; `internal/kafkarest/confluent` implementation;
`internal/kafkarest/mock` test double; `cluster.BuildKafkaRESTClient` near `BuildMDSClient`.
Reuse HTTP/TLS/auth plumbing and `internal/httpretry` where applicable.

### REST consistency, pagination, and error handling

- Follow `metadata.next` on **all paginated list endpoints** to completion. Honor the API's
  returned next link safely, without leaking credentials to an untrusted origin.
- Never use a partial page set as a complete mirror index. Duplicate/conflicting mirror entries,
  missing required fields, and unknown response shapes are errors.
- `401/403`, timeout, `429`, `5xx`, malformed JSON, unsupported endpoint, or incomplete list
  produce an **unknown live state** and an explicit diagnostic, not an empty list.
- A missing specific link or mirror is `NotFound` only on an authoritative response (e.g. a valid
  `404` or a complete list), not when a request failed.
- GETs may be retried with bounded exponential backoff, retry budget, and request timeout;
  `Retry-After` is honored where applicable. The finalizer's in-lock verification is an
  explicit exception: one total deadline, no retries, and no `Retry-After` sleep under the
  lock. A `429` there fails the attempt as `Unknown`.
- Mutating POSTs are **not blindly retried** after an uncertain outcome. If creation or config
  alteration times out, read the live state again and perform normal diff on the next attempt.
  Never assume a timeout means the write failed.
- An inaccessible REST service on a `KafkaCluster` with `rest` configured blocks **mutating
  topic/mirror operations whose safe classification requires that snapshot**. A read failure is
  surfaced in `diff`, `apply`, `verify`, and operator status. Avoid making unrelated clusters fail.
- Build one **consistent-enough, immutable per-cluster snapshot** for each CLI run or operator
  reconciliation/batch. The snapshot is not a transaction: after any create uncertainty, re-read
  before taking another action.
- **Plan time is not write time.** Before each mirror creation or topic-config alteration, perform
  a just-in-time targeted read of the destination topic/mirror identity and applicable link
  state/config (plus REST/Admin ID check where not yet verified in that execution). If new
  evidence invalidates the plan, stop or replan; never apply an old `Absent` classification after
  discovering a live normal topic. Avoid reuse of snapshots across runs or reconciles, and
  invalidate relevant cached topic classification after successful writes.
- An existing `KafkaTopic` deletion finalizer has a separate, stricter pre-cleanup and
  pre-`DeleteTopic` verification contract below. Its final verification uses a fresh complete
  all-links mirror index, not merely a known-link describe. Pagination is part of one logical
  attempt and shares one deadline. Neither read/write sequence is an atomic guarantee
  against an out-of-band Confluent lifecycle transition.

## Shared topic classification and live-state snapshot

A single classification function consumes the existing Kafka Admin topic inventory plus complete
REST mirror/link data for a destination cluster. All consumers — CLI diff/apply/verify, import,
`KafkaTopic` guard, and operator — must use it rather than reimplementing checks.

Suggested conceptual model (exact Go names left to implementation):

```go
// A complete, immutable view of one destination cluster for one planning pass.
type ClusterTopicSnapshot struct {
    ResolvedClusterRef string                    // canonical cluster namespace/name or CLI ref
    AdminClusterID     string                    // from Kafka Admin protocol
    RESTClusterID      string                    // configured/discovered Confluent REST target
    Topics             map[string]KafkaTopicLive // from Kafka Admin
    Mirrors            map[string]MirrorLive     // complete REST pagination
    Links              map[string]LinkLive       // state + required configs
    Complete           bool                      // false => no safe mutation plan
}

type TopicClassification string
const (
    TopicAbsent            TopicClassification = "Absent"
    TopicNormal            TopicClassification = "Normal"
    TopicMirror            TopicClassification = "Mirror"
    TopicStoppingMirror    TopicClassification = "StoppingMirror"
    TopicStoppedFormer     TopicClassification = "StoppedFormerMirror"
    TopicUnknown           TopicClassification = "Unknown"
)
```

The model is conceptual; it does not prescribe `internal` package boundaries or serialization.
The snapshot must distinguish `unknown` from `absent` as a **type-level invariant**. `Complete`
is true only after required Kafka and REST reads have succeeded **and**
`AdminClusterID == RESTClusterID`. A mismatch yields `ClusterIdentityMismatch`; it must not be
reported as a topic-specific `Absent`.

Classification order (always inspect the exact REST `mirror_status` before generic `PENDING_*`):

| Evidence | Classification | Meaning / protection |
|---|---|---|
| Kafka topic absent; no REST mirror entry; complete reads | `Absent` | Mirror may be created if link and prefix also validate |
| Kafka topic exists; no REST mirror entry; complete reads | `Normal` | Must not be overwritten by `KafkaMirrorTopic` |
| REST mirror with `ACTIVE`, `PAUSED`, `FAILED`, `LINK_*`, `SOURCE_UNAVAILABLE`, or non-stopping pending state | `Mirror` | Normal `KafkaTopic` changes are rejected |
| REST mirror with `PENDING_STOPPED` | `StoppingMirror` | Protected; not imported as either kind, not mutated |
| REST mirror with `STOPPED` **and** Kafka topic exists | `StoppedFormerMirror` | Writable normal topic retaining historical mirror metadata |
| REST mirror with `STOPPED` but Kafka topic absent, or Kafka/REST inconsistency | `Unknown` | Diagnostic; no create/adoption without re-check |
| Required REST/Kafka read fails, incomplete pagination, unsupported/unknown status | `Unknown` | Fail closed for affected mutations |

Other non-stopping pending states include `PENDING_MIRROR`, `PENDING_SYNCHRONIZE`,
`PENDING_SETUP_FOR_RESTORE`, and restore variants `PENDING_RESTORE` (REST schema) and
`PENDING_RESTORE_MIRROR` (documented lifecycle terminology, where observed). Do not assume the
REST enum and prose documentation use identical names across platform versions; accept known
variants, preserve the raw status, and treat any **unrecognized** status as `Unknown`, not healthy.

A `StoppedFormerMirror` is eligible for `KafkaTopic` import and (absent an active
`KafkaMirrorTopic` claim) can be managed by `KafkaTopic`. A `KafkaMirrorTopic` declaration for it
reports lifecycle/identity drift and **never** creates a replacement or converts it back.

For cross-kind ownership, every resource uses the canonical destination claim
`(resolved KafkaCluster reference, effective topicName)`. The operator computes the reference as
`(ClusterNamespace != "" ? ClusterNamespace : resource.namespace) + "/" + clusterRef.name`;
it **does not** use `clusterRef.name` alone. This allows two distinct namespace-local
`KafkaCluster/dr` objects to host independent topics named `payments.orders`, while two CRs in
different namespaces referencing the *same* global `KafkaCluster/dr` compete for one topic.
CLI identity resolution must likewise use its actual resolved cluster definition rather than a
bare ambiguous display name.

The snapshot does not grant ownership; it supplies physical state to a separate duplicate-claim
index. Both the claim index and the physical classification must allow an operation. The
identity comparison, candidate-scan scope, lock key and authoritative recheck must agree across
both resource kinds. The identity key is not kind-qualified.

## The KafkaMirrorTopic kind

```yaml
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaMirrorTopic
metadata:
  name: orders-dr
  namespace: payments
spec:
  clusterRef: { name: dr }            # destination cluster: the link lives here
  linkName: prod-to-dr                # pre-existing destination-side link
  sourceTopicName: payments.orders    # source-cluster topic
  topicName: payments.orders          # optional; defaults to sourceTopicName
  replicationFactor: 3                # optional; CREATE-TIME ONLY
  config:                             # optional local, non-mirrored config keys
    min.insync.replicas: "2"
  reconciliation: { mode: Enforce }   # Enforce | DetectOnly | ObserveOnly
  deletionPolicy: Orphan              # optional; the only allowed value in v1
```

Removing the manifest (CLI) or deleting the CR (operator) always leaves the Kafka topic in place.
`deletionPolicy` exists in the schema **only so that `Delete` can be rejected**: it is declared with
`+kubebuilder:validation:Enum=Orphan` (CRD) and checked in offline validation (CLI), so
`deletionPolicy: Delete` fails admission/validation instead of silently becoming `Orphan`. Leaving
the field out of the schema would not achieve this in the operator, because the API server prunes
unknown fields before the webhook runs. A future API revision may add `Delete` only after its race
and promoted-topic safety model is resolved.

### Identity, validation and prefix

- `clusterRef`, `linkName`, `sourceTopicName`, and the **effective** `topicName` are immutable.
  For the new kind, enforce the effective-name invariant **at `spec` scope in CRD CEL**, not only
  in a webhook and not by copying the optional-field rule from `KafkaTopic`. Conceptually:
  `effectiveName(spec) = has(spec.topicName) ? spec.topicName : spec.sourceTopicName`;
  require `effectiveName(oldSelf) == effectiveName(self)`. With
  `sourceTopicName` itself immutable, an omitted `topicName` can become explicit **only** with the
  same effective value. Changing to a different destination name requires a replacement CR.
  Mirror the exact rule in webhook and CLI validation; test the generated CRD with webhooks
  disabled. A spec-scoped CEL sketch (subject to code-generator/envtest verification):

  ```cel
  (has(oldSelf.topicName) ? oldSelf.topicName : oldSelf.sourceTopicName) ==
  (has(self.topicName) ? self.topicName : self.sourceTopicName)
  ```

  Disallow empty explicit topic names or define them equivalently to omission consistently across
  CRD, webhook and CLI (preferred: reject empty explicit values).
- There is no `partitions` field: partition growth follows the source.
- Effective destination name = explicit `topicName` or `sourceTopicName` if omitted. This is
  always determined offline for tenancy and duplicate checks.
- The destination `topicName` follows the same namespace prefix/tenancy restrictions as
  `KafkaTopic.topicName`.
- Runtime validation reads the **actual** `cluster.link.prefix` from the destination link via
  `GET .../links/{link}/configs`. If set, the destination name must be exactly
  `prefix + sourceTopicName`, and `topicName` must be explicit. Without a prefix, it must match
  `sourceTopicName`. A mismatch prevents create/update; inability to read the prefix also blocks
  creation/update rather than assuming no prefix.
- Link existence, destination direction, and state are checked at runtime. A missing link is a
  resource error; an unobservable link is an unknown-state error; an unhealthy link is reported
  explicitly and no unsafe create is attempted.
- Identity `(resolved KafkaCluster, effective topicName)` is **shared with `KafkaTopic`**.
  CLI collision is a hard validation error, including duplicates across resource kinds resolving
  to the same cluster. Operator webhook rejects the newer conflicting claim; reconcile also
  guards against a conflicting claim if a webhook is bypassed or two admissions race. The
  candidate-scan scope follows the resolved cluster namespace rule; identical `clusterRef.name`
  is **not** enough for a collision when the CRs resolve to different KafkaCluster objects.
  Tie-breaking is deterministic and extends the repository's existing order (see "Operator
  concurrent admission").

### Configuration and replication factor

- `config` contains only **declared local keys**. The tool compares only those keys to the live
  destination topic. It must not write undeclared keys, or make a synced config locally owned.
- The set of synced configs depends on link configuration (`topic.config.sync.include`) and
  Confluent version. This design does **not** hardcode a universal mirror-config allowlist.
- Before updating a declared key, use available link configuration to detect known synchronized
  keys. If a key is known to be link-managed, return `Rejected` / a resource validation error
  instead of repeatedly trying writes. If ownership cannot be established, prefer a diagnostic
  and a safe failure for that key. Surface broker validation errors unchanged when the server
  ultimately decides writability.
- `replicationFactor` is optional and applied **only at creation**. On an existing mirror, an
  explicitly declared RF mismatch is non-executable drift (`UnsupportedReplicationFactorChange`),
  visible in diff/verify/status. Do not generate `UpdateReplicationFactor`.
- An omitted RF does not imply managing any particular RF; importer may explicitly record the
  observed destination RF as a create-time declaration.

## Diff, diagnostics and verify contract

The planner must expose **both** executable operations and non-executable findings. This can be
implemented by extending `internal/operations` with report-only diagnostics or by a parallel
`Findings` type that existing diff/verify/output consumers understand. Avoid overloading
`Rejected` for a healthy progressing state or for every diagnostic.

Suggested finding classes:

- `ActionableDrift`: `CreateMirrorTopic` or `UpdateMirrorTopicConfig` can be performed.
- `UnhealthyDrift`: `MirrorTopicUnhealthy` / `MirrorTopicPromoted` / `MirrorTopicStalled`;
  observed, never executed automatically.
- `UnsupportedDrift`: e.g. `UnsupportedReplicationFactorChange`.
- `Rejected`: wrong existing topic kind, wrong link/source identity, or a known forbidden write.
- `Progressing`: a recognized transitional mirror state; informational during an allowed window.
- `StateReadError`: incomplete/unavailable/contradictory live state, including
  `ClusterIdentityMismatch`; no unsafe operations.

Each finding carries resource identity, observed/desired details, human-readable reason, a stable
machine-readable code, and a flag indicating whether it counts as drift or operational error.
Do not represent an unavailable REST snapshot as `CreateMirrorTopic`.

| Observed situation | Diff output | Apply behavior |
|---|---|---|
| Topic absent; link exists/healthy; prefix matches; reads complete | `CreateMirrorTopic` | Create in `Enforce` |
| Existing normal or stopped former mirror at that name | `Rejected` | Never replace or create |
| Existing mirror on a different link or with another source topic | `Rejected` | Never rebind |
| Matching mirror, declared writable config differs | `UpdateMirrorTopicConfig` | Alter only declared keys in `Enforce` |
| Explicit RF mismatch on existing mirror | `UnsupportedReplicationFactorChange` | Never alter RF |
| Link missing / wrong prefix / known synced config declaration | `Rejected` or resource error | Do not create/update |
| REST/Kafka state read failed, incomplete, ambiguous or refers to different cluster IDs | `StateReadError` (`ClusterIdentityMismatch` for mismatch) | Fail closed |
| Mirror exists and matches, status `ACTIVE` | No finding | No op |
| Status `PAUSED`, `LINK_PAUSED`, `FAILED`, `LINK_FAILED`, `SOURCE_UNAVAILABLE` | `MirrorTopicUnhealthy` | Report; never resume |
| Status `PENDING_STOPPED` or `STOPPED` | `MirrorTopicPromoted` / `MirrorTopicStopping` drift | Report; never recover by creating |
| Other known `PENDING_*` within progress window | `Progressing` | Observe; no lifecycle mutation |
| Known transitional state past stall threshold | `MirrorTopicStalled` | Report; never force lifecycle |

`DetectOnly` and `ObserveOnly` produce findings and mark executable operations report-only,
consistent with existing reconciliation modes. They do not execute writes. A blocked/rejected
operation must never be made executable by changing the reconciliation mode.

### Lifecycle state and stalled transitions

Exact status matching must check `STOPPED` and `PENDING_STOPPED` **before** generic pending states:

| REST `mirror_status` | Semantic state | Verify in `Enforce` / `DetectOnly` |
|---|---|---|
| `ACTIVE` | Healthy | Pass unless other drift |
| `PAUSED`, `LINK_PAUSED` | Unhealthy: paused | Fail (drift) |
| `FAILED`, `LINK_FAILED`, `SOURCE_UNAVAILABLE` | Unhealthy: failed/unavailable | Fail (drift) |
| `PENDING_STOPPED` | Becoming a normal topic | Fail (drift), never import/adopt |
| `STOPPED` | Already normal topic, former mirror | Fail for a `KafkaMirrorTopic` declaration |
| Known other `PENDING_*`, within window | Progressing | No drift failure solely for status |
| Known other `PENDING_*`, stalled | Stalled | Fail (drift) |
| Unknown value or REST error | Unknown/read error | Fail (operational error) |

`ObserveOnly` does not fail **for drift**, following the existing rule; actual read/connection
errors should still be reported as operational errors and must not be mistaken for successful
verification. Apply the same exit-code/condition taxonomy as other resource kinds.

For `MirrorTopicStalled`, the operator persists a `lastObservedStatusChangeTime` / equivalent in
status so repeated reconciles (and controller restarts) can identify a transition stuck beyond a
**30-minute default threshold**. The threshold should be configurable as an operator setting,
not a v1 per-resource CRD field. The CLI has no durable state and reports `Progressing` unless
Confluent supplies a reliable status timestamp sufficient to show the threshold has passed.
`PENDING_STOPPED` counts as drift immediately regardless of duration.

Include a REST-provided reason (`mirror_topic_error`, state-transition error where available,
and/or link state) in diagnostics without leaking credentials.

## CLI behavior

### `validate`

- Verify schema, defaulting, tenancy, immutability, effective identity, and collisions within
  `KafkaMirrorTopic` and across `KafkaTopic`.
- Require `KafkaCluster.rest` for every referenced mirror destination.
- Do not call REST for offline identity checks; link existence/prefix/config checks happen in
  `diff`, `apply`, `doctor`, and operator reconcile.

### `diff` / `apply` / `verify`

- Resolve the canonical KafkaCluster reference for each manifest and verify Kafka Admin cluster
  ID equals REST cluster ID before combining the two sources of live state.
- Build one complete classification snapshot per destination cluster (with necessary link details)
  before planning topic/mirror mutations.
- Feed the same classification to `KafkaTopic` guard and `KafkaMirrorTopic` diff.
- Separate executable operations from lifecycle findings and read errors.
- `apply` executes only `CreateMirrorTopic` and safe `UpdateMirrorTopicConfig` via REST, after
  targeted just-in-time classification/link validation for each affected topic rather than
  trusting a stale planning snapshot.
- For an uncertain POST result, re-read rather than retrying create indiscriminately.
- `verify` fails for drift in `Enforce` / `DetectOnly`, including lifecycle-only drift; it observes
  the existing `ObserveOnly` exemption. Read errors are reported distinctly.
- Removing a CLI manifest never deletes a mirror (consistent with no mirror `DeleteTopic` op).

### `import`

For each destination cluster with a usable `rest` block:

1. Resolve the destination KafkaCluster reference; verify the Kafka Admin and REST cluster IDs
   match, then read the complete Kafka topic inventory and all paginated REST mirrors, plus link
   metadata.
2. Compute physical classification by **effective destination name**, including historical
   `STOPPED` entries.
3. Import `Mirror` classifications as `KafkaMirrorTopic` (including unhealthy states, with
   diagnostic). Preserve link name, source topic, destination topic, and observed RF.
4. Import `StoppedFormerMirror` with an existing Kafka topic as `KafkaTopic`; do **not** lose it
   merely because REST still lists it as a former mirror. First check for an active
   `KafkaMirrorTopic` claim on the same resolved cluster/topic. Do not silently transfer ownership
   while that claim exists: report an import conflict and require an explicit manifest migration
   (remove/orphan former mirror claim, then adopt normal topic) before emitting a competing
   `KafkaTopic` into a managed desired-state set.
5. Do not import `StoppingMirror` as either kind: emit a clear deferred/import-incomplete
   diagnostic and require another run after promotion finishes.
6. Treat `Unknown` or contradictory Kafka/REST evidence as an error/incomplete import, not an
   opportunity to guess the resource kind.
7. Ensure no physical topic is imported twice as `KafkaTopic` and `KafkaMirrorTopic`.

**Config import policy (v1):** do not automatically claim all observed mirror topic configs.
Since topic-config provenance can be ambiguous, omit `KafkaMirrorTopic.spec.config` by default
and emit a diagnostic describing configs that may need explicit, reviewed adoption. Operators may
add verified local keys manually. The importer can be enhanced later once it can prove which
keys are unsynchronized across supported link versions. Identity/RF import is a supported round
trip; automatic preservation of all local config ownership is **not** promised in v1.

**ACL import policy (v1):** inspect `acl.sync.enable` on the associated link:

- When ACL sync is conclusively **disabled**, ACLs attached to an imported mirror topic may be
  emitted as a standalone `KafkaAccessPolicy` using the existing ACL importer, preserving
  local policy in the manifest round trip.
- When ACL sync is **enabled** (or its value cannot be determined), Kafka ACL listing cannot
  reliably distinguish synchronized ACLs from locally managed ACLs. Do not silently adopt them
  into GitOps; emit a prominent `ExternallyManagedOrAmbiguousACLs` diagnostic and mark the import
  as partial for those ACLs. Local-only ACLs may require manual declarations.
- Never pretend an import with skipped config/ACL ownership is a lossless full-resource round
  trip. Document partial output and ensure automation can detect its diagnostics.
- This rule does not alter existing `KafkaAccessPolicy` reconciliation semantics for explicitly
  authored policies.

### `doctor`

- Check REST endpoint, auth, TLS, cluster ID resolution, complete pagination, and required
  destination-side list/describe permissions. Obtain the Kafka Admin cluster ID and verify it
  matches the configured/discovered REST cluster ID; report `ClusterIdentityMismatch` with both
  identifiers (without exposing credentials).
- Check each referenced link exists, is in the expected destination, and expose its current
  state and relevant config values.
- Check prefix/name consistency; report an unknown/unreadable config as a failure.
- Warn if `auto.create.mirror.topics.enable=true` because the link may create mirrors outside Git.
- Report that create/alter authorization may not be fully testable without performing mutations;
  do not claim write permissions were validated when only read checks ran.
- Warn on clusters lacking `rest` that the out-of-band `KafkaTopic` mirror guard is unavailable.
- When practical, detect two distinct KafkaCluster configurations referring to the same Kafka
  cluster ID and warn that cross-CR physical aliasing is not covered by the CR-scoped ownership
  key.

## Kubernetes operator

- Add `kafkamirrortopics.gitops.monedula.dev` CRD, controller, reconcile, validating webhook,
  status, metrics, RBAC, Helm templates, examples, and generated manifests in the repository's
  established layout.
- Reconcile from the same shared diff/live-state model as the CLI, not a different lifecycle
  interpretation.
- Status carries `phase`, conditions, `observedMirrorStatus`, `linkName`, `sourceTopicName`,
  `drift`, `lastCheckedTime`, `lastAppliedTime`, and sufficient transition timing information for
  stalled-state detection. Use established status/condition naming patterns.
- Requeue on the existing resync interval to detect `PAUSED`, `FAILED`, `STOPPED`, and link state
  changes without any Git edit.
- `StateReadError` sets an observable error condition; it never means the destination topic is
  absent. No unsafe mutation follows.
- Deleting a `KafkaMirrorTopic` CR **always orphans** its Kafka topic. No mirror-deleting REST
  call is made. A finalizer, if needed for bookkeeping, must never block on a Kafka deletion or
  perform one.
- The webhook performs cross-kind duplicate checks; reconcile performs the same check as a
  fallback against races and webhook bypass. A collision remains blocked until resolved. Both
  use resolved KafkaCluster namespace/name; `ClusterNamespace` override behavior must match the
  existing `KafkaTopic` lookup logic.
- Update per-identity locking to serialize cross-kind claimants with a **shared**
  `(resolved KafkaCluster, topicName)` key, rather than `(KafkaCluster, kind, topicName)`
  (today `locking.Registry.LockIdentity` includes the kind). This applies to the regular reconcile
  paths; the `KafkaTopic` deletion finalizer keeps taking no identity lock (see the finalizer
  guard).
  Keep the existing cached scan plus contested-path uncached `APIReader` recheck as the baseline;
  extend both to include cross-kind claimants, including young objects missed by the cache.
- Rebuild or invalidate REST client credentials on watched Secret updates using the same
  operator dependency/wiring pattern as existing cluster auth sources; do not retain stale
  passwords or tokens across relevant resyncs.
- Keep `KafkaTopic` reconcile **and its `Delete` finalizer** protected by the same mirror
  classification/ownership guard.

### Operator concurrent admission

Two kinds can be admitted concurrently before either webhook sees the other. The webhook alone
is therefore not a sufficient ownership invariant. A shared *resolved cluster/topic* claim resolver runs
in reconcile and prevents **both** controllers from performing unsafe mutations while ownership
is ambiguous. It must scan both resource kinds with the same resolved cluster namespace rule,
and use the same cross-kind lock key when checking and mutating state.

The winner is chosen by the repository's existing strict total order (`olderIdentityClaim`,
`internal/operator/controller/duplicate.go`), extended with the kind as the last key. Only
claimants resolving to the **same KafkaCluster namespace/name** enter this comparison:

1. earlier `creationTimestamp` wins;
2. on equal timestamps (1 s granularity), lexicographically smaller `namespace/name` wins;
3. on equal `namespace/name` (possible only across kinds), `KafkaTopic` wins over
   `KafkaMirrorTopic`.

The order is total over distinct objects, so exactly one claimant wins and every other claimant is
blocked with a conflict condition until the collision is resolved. The same comparison is used by
the webhook and by the reconcile-time fallback, for same-kind and cross-kind duplicates alike, so
both controllers always agree on the winner. Only when a claimant's identity cannot be read (API
error) is it treated as ambiguous: the reconcile blocks mutation and retries, as
`DuplicateCheckFailed` does today. The authoritative contested-path recheck and per-identity
lock must cover **both** kinds; extending only the timestamp comparator is insufficient.

(Preferring `KafkaTopic` in step 3 is a fixed convention, not a judgment about which is right; it
only needs to be deterministic. It is the conservative choice because a `KafkaTopic` facing a live
mirror is rejected by the mirror guard anyway.)

## KafkaTopic mirror guard (CLI and operator)

When a destination `KafkaCluster` has REST configured:

- `KafkaTopic` targeting `Mirror` or `StoppingMirror` is `Rejected` before topic partition,
  config, RF, schema, or access-derived mutations concerning that physical topic.
- `KafkaTopic` targeting `StoppedFormerMirror` may manage the now-normal topic only when no
  active conflicting `KafkaMirrorTopic` declaration claims the same canonical
  `(resolved KafkaCluster, effective topicName)`.
- `KafkaTopic` targeting `Unknown` is blocked with `StateReadError` rather than planned as a
  normal topic. In particular, a failed `links/-/mirrors` request never becomes an empty set.
- Guard evaluation must use **complete pagination** and the shared snapshot. One logical list
  pass per cluster per run/reconcile is preferred; extra describe calls are allowed when needed
  to disambiguate state, but cannot weaken fail-closed behavior. The preference for one
  planning-time list pass does not remove the finalizer's required fresh all-links verification.

Without `rest`, `KafkaTopic` retains existing legacy behavior; the guarantee of mirror
recognition does not apply, and the limitation is documented in `doctor`. Mirror resources are
not allowed without REST.

### KafkaTopic `deletionPolicy: Delete` finalizer guard (v6)

The existing `KafkaTopic` finalizer can remove **topic, ACLs, and Schema Registry subjects**
after the required `gitops.monedula.dev/allow-delete=true` approval. Its deletion path is separate
from ordinary diff and the normal duplicate winner gate. V6 defines the following precedence
and verification contract; it replaces the v4/v5 finalizer algorithm without adding a mirror
Delete operation.

#### Decision precedence and co-claimant semantics

A deletion co-claimant is another active, non-terminating `KafkaTopic` or `KafkaMirrorTopic` CR
with the same canonical `(resolved KafkaCluster, effective topicName)` key. Exclude the deleting
CR itself. Use the same namespace-local/global resolution and scan scope as regular reconcile.
This is a preservation check, not an election: an active duplicate protects the resource even
when it would lose the regular timestamp/name/kind tie-break.

Scan both kinds using the existing cached-scan plus uncached `APIReader` negative-result
confirmation pattern. A positive matching claim is sufficient to preserve resources. A negative
result must be complete for both kinds and the relevant scope; a failed or incomplete List/read
is `ClaimCheckFailed`, not evidence that no claimant exists.

| Decision, in priority order | External cleanup | Finalizer outcome |
|---|---|---|
| Effective policy `Orphan`, or existing force-removal escape hatch | No topic, ACL, or schema cleanup; no Kafka/REST prerequisite | Remove this controller's finalizer |
| Positive active co-claimant | Full orphan: skip all remaining topic, ACL, and schema cleanup; no Kafka/REST prerequisite | Warning naming the claimant; remove this controller's finalizer |
| Claim scan fails or cannot confirm a negative result | No cleanup | Retain finalizer; report claim-read error and requeue |
| No claimant; destructive approval is missing | No cleanup | Retain finalizer under the existing approval gate |
| No claimant; REST configured; classification `Mirror` or `StoppingMirror` | No cleanup may start | Retain finalizer; report `MirrorDeleteBlocked` |
| No claimant; REST configured; classification `Unknown` or cluster binding fails | No cleanup may start | Retain finalizer; report state-read/binding error |
| No claimant; classification `Normal` or `StoppedFormerMirror` | Eligible only after approval, ownership checks, and final verification | Complete cleanup or retain on failure |
| No claimant; classification `Absent` | Skip `DeleteTopic`; only eligible remaining cleanup may run | Remove finalizer once required remaining work completes |

**The positive co-claimant branch runs before Kafka/REST discovery, authentication, cluster-ID
checks, and mirror classification.** It works for a live `Mirror`, `StoppingMirror`, normal topic,
or unavailable REST service alike. A destination outage must not prevent a no-cleanup orphan.
No delete approval is needed to leave all external resources untouched. Patching the Kubernetes
finalizer can still fail and is retried without starting cleanup.

Full orphan means no additional external mutations by this finalizer: no `DeleteTopic`, ACL
revocation, schema-subject deletion, or compensating recreation. Emit a warning naming the
claimant and retained resource categories, and remove only this controller's finalizer. Do not
change the competing CR or imply that ownership was transferred. If earlier attempts already
performed partial cleanup, orphaning stops further work; it cannot undo prior mutations.

For a normal-to-mirror migration, a retained normal topic still prevents mirror creation.
Finishing the old CR's finalization is not a conversion or a promise that the new mirror will
become ready; an operator must resolve the physical-topic conflict explicitly.

#### Ordered cleanup path

1. **Take no-cleanup exits first.** Effective `Orphan` and the existing orphan-only force-removal
   escape hatch require no Kafka/REST connection and no external cleanup. For policy `Delete`,
   resolve the canonical claim key and run the co-claimant scan above before constructing
   destination clients. A positive result completes full orphan; a failed scan keeps the finalizer.
2. **Prepare outside the ACL substrate lock.** With a confirmed negative claim result, require
   the existing destructive approval and cleanup-policy gates. When REST is configured, verify
   Kafka Admin/REST destination cluster binding and obtain fresh classification before entering
   `deleteTopicState`. Read the target topic through Kafka Admin and the complete all-links mirror
   index through REST. `Mirror`, `StoppingMirror`, or `Unknown` blocks every initial cleanup
   action, including ACLs and subjects. `Normal`, `StoppedFormerMirror`, and `Absent` can proceed
   subject to the steps below.
3. **Keep the existing lock discipline.** The deletion path takes the cluster's ACL substrate
   lock, but **no identity lock**. Preserve existing ownership protections for individual ACLs
   and schema subjects; absence of a topic co-claimant does not prove exclusive ownership of
   every dependent resource. For an initially normal/former topic, existing authorized ACL
   cleanup may run after the preflight guard. For an initially `Absent` topic, defer remaining
   cleanup until the bounded verification below has reconfirmed absence. If a later ownership
   check discovers a topic co-claimant, stop further work and use full orphan.
4. **Run one bounded authoritative verification at the final safety boundary.** For a topic
   expected to exist, place it immediately before the Kafka `DeleteTopic` request, inside the
   deletion implementation rather than only in controller preflight. For an initially `Absent`
   topic, place it before the first remaining destructive cleanup action, since there will be
   no `DeleteTopic`. Use the all-links algorithm and shared total deadline below.
   - `Normal` / `StoppedFormerMirror`: a pending delete may proceed only if all other gates still
     allow it. If preflight was `Absent` but a topic now exists, abort this attempt and requeue
     for a fresh preflight; do not turn an absence-recovery path into an unplanned delete.
   - `Absent`: the topic-deletion step is already complete; skip `DeleteTopic` and consider only
     remaining approved, ownership-safe cleanup.
   - `Mirror` / `StoppingMirror` / `Unknown`, timeout, or invalid cluster binding: do not issue
     `DeleteTopic` and do not start any further cleanup. Retain the finalizer, report the reason,
     release the ACL lock, and requeue. If ACL cleanup already happened, report that partial
     outcome explicitly rather than claiming that nothing was changed.
5. **Complete or retry idempotently.** After successful deletion or confirmed absence, finish
   only the remaining cleanup authorized by policy and its ownership checks, then remove the
   finalizer. A failed or uncertain `DeleteTopic` result does not prove absence: retain the
   finalizer and classify again on the next attempt. Do not replay deletion solely because the
   CR is still terminating, and never recreate a topic to satisfy a cleanup precondition.

When REST is not configured, the legacy KafkaTopic deletion behavior and its limits remain:
there is no claim of out-of-band mirror recognition. The co-claimant/full-orphan precedence still
applies, and authoritative Kafka absence can complete the legacy topic-deletion step. Do not
silently fall back to this unguarded path when REST is configured but unavailable.

#### Authoritative final verification algorithm

The v6 baseline is deliberately implementable using endpoints already listed in this design.
The input is the canonical claim key, the exact effective topic name, and clients whose unchanged
configuration has passed destination cluster binding in this attempt. The output is the shared
`TopicClassification` plus evidence/error details, not a boolean inferred from topic existence.

1. Use a fresh `GET /v3/clusters/{cluster_id}/links/-/mirrors` request and follow **every**
   `metadata.next` page to completion within the total deadline. Validate origin/path safety,
   required fields, statuses, duplicates, and page progression as in the normal snapshot reader.
   A pagination loop, invalid next link, conflicting entry, or incomplete pass is `Unknown`.
2. Read the exact destination topic's existence/metadata through Kafka Admin as part of this
   same verification attempt. Do not reuse planning-time topic existence. Check that the
   destination binding still applies to these clients; a config/client/credential change
   invalidates the attempt and requires fresh preparation outside the lock.
3. Classify the topic using the shared rules and the **complete** new mirror index across all
   links. A negative match is usable only after all pages succeeded. A matching `STOPPED` entry
   is a former mirror only when Kafka confirms the topic exists. `PENDING_STOPPED`, unknown
   statuses, and contradictory evidence remain protected. Do not fetch unrelated link configs
   in this critical section: they are not evidence that a topic is a normal topic.

A known-link `GET .../links/{link}/mirrors/{mirror}` can confirm a mirror on that link, but its
negative result cannot exclude a mirror on another link. A Kafka or REST response that proves
only topic existence is likewise insufficient to prove non-mirror status. Therefore neither
replaces the complete all-links pass. Do not assume that `-` is supported as a wildcard by the
single-mirror describe endpoint, or that an undocumented topic-name filter is complete.

A future optimized lookup is permitted only after its cross-link coverage, all relevant status
values, and authoritative negative results are documented and integration-tested for each
supported version. The full-list baseline requires no such optimization. No endpoint or
pagination sequence is treated as an atomic cluster snapshot.

#### Whole-attempt deadline and ACL lock impact

The operator exposes a positive, finite finalizer verification budget (conceptual setting name:
`finalizerVerificationTimeout`; proposed default **5 seconds**). It is an operator setting, not a
per-resource API field. The default is a v6 design choice to be validated in compatibility/load
tests, not a statement about expected cluster latency. An invalid, zero, or unlimited value is
rejected. The effective deadline is the earlier of this budget and the remaining reconcile
deadline.

Start the budget once when final verification begins. It covers **the entire logical attempt**:
all REST pages, the Kafka Admin read and any necessary binding check, network/body reads, and
response validation. Do not reset it for the next page or give each request a fresh full budget.
Propagate cancellation to the clients and check it while processing responses and page chains.
A result completed after the deadline is not valid deletion evidence.

There are **no request retries, backoff, or `Retry-After` sleeps** in this verification path;
requesting the next page is pagination, not a retry. Disable any generic REST retry wrapper for
these calls. Timeout or any failed page yields `Unknown`; release the lock and schedule a later
reconcile rather than starting another verification attempt in the same critical section.

Preparation, broad discovery, and ordinary retrying reads remain outside the ACL lock. The final
verification intentionally runs under it. Other ACL writers **may wait for its remaining
budget**, as well as existing cleanup work; this design bounds the added verification delay,
not the total duration of every cleanup operation. Tests must demonstrate deadline propagation,
lock release on every error path, and a waiting ACL writer's ability to continue afterward.
Do not claim that bounded REST I/O has zero effect on other writers.

#### Absence, partial cleanup, and residual races

Confirmed `Absent` has exactly the shared-classifier meaning: Kafka confirms no topic and the
complete REST index contains no contradictory mirror evidence. A topic missing from Kafka but
still associated with a conflicting/historical mirror entry remains `Unknown` under the existing
rules; this revision does not silently weaken that classifier. A local memory of having sent
`DeleteTopic`, a timeout, or a single-link `404` never substitutes for fresh absence evidence.

A restart after a successful delete but before finalizer removal must be able to resume from
confirmed `Absent`. It skips the already-completed topic step, safely repeats or skips residual
cleanup using existing idempotency/ownership checks, and removes the finalizer when all required
work is complete. Missing optional dependent resources count as already cleaned only according
to their existing authoritative-not-found handling. A remaining ownership/read failure retains
the finalizer; absence does not authorize deleting a shared ACL or subject.

If a late verification failure follows ACL cleanup, emit a partial-cleanup diagnostic describing
known completed stages and the blocked stage. Do not execute schema cleanup after that failure
or automatically recreate revoked ACLs. A subsequent reconcile starts with claim precedence and
fresh preflight again. Full orphan on a later attempt stops further work but cannot restore
previously removed state.

These guards prevent **known** unsafe deletion. They are not an atomic claim lease, Kafka/REST
snapshot, or compare-and-delete primitive. A co-claimant or replacement topic can appear after
a check, and an external lifecycle transition can race with `DeleteTopic` after final
verification. The finalizer takes no cross-kind identity lock; the ACL lock is not a distributed
lock against external actors. Keep these residual TOCTOU limits explicit in operator docs and
tests. The default remains `Orphan`, and `KafkaMirrorTopic` deletion never calls topic, ACL, or
schema cleanup in v1.

## Testing and acceptance criteria

### Unit and component tests

- REST client: authentication modes, TLS, URL/path escaping, REST base URL, cloud `clusterId`,
  `401/403/429/5xx`, timeout, malformed JSON, unknown enum, complete pagination, invalid next
  link, duplicate data, bounded retry, uncertain POST outcome, and correct mirror create
  `configs: [{name, value}]` wire encoding.
- Classification: every lifecycle status including `STOPPED`, `PENDING_STOPPED`,
  `PENDING_RESTORE` and `PENDING_RESTORE_MIRROR`; Kafka/REST contradictions; no topic; normal
  topic; missing page; unknown status; wrong-link identity, REST/Admin cluster-ID agreement,
  explicit ID mismatch, unavailable ID and wrong REST endpoint despite syntactically valid IDs.
- Diff/verify: healthy mirror with no ops, lifecycle-only drift, progressing versus stalled,
  explicit RF mismatch, config drift, wrong link/source, prefixed link, unsupported config key,
  all reconciliation modes and error exit semantics.
- Import: no double-import; `STOPPED` becomes `KafkaTopic` only when no active competing mirror
  declaration claims it; `PENDING_STOPPED` deferred; `Unknown` fails; local-vs-synced configs and
  ACL policies generate correct diagnostics.
- Operator: webhook + reconcile duplicate checks, bypassed admission, simultaneous admissions,
  namespace-local versus global KafkaCluster resolution, shared cross-kind locks and quorum
  rechecks, KafkaTopic deletion-finalizer classification guard (including ACL/schema ordering),
  orphan-only mirror CR deletion, Secret credential rotation, and status timing across restarts.
- Finalizer verification: complete all-links reads when no link is known, changed-link
  replacement, absence recovery after restart, positive co-claimant precedence over REST
  outages, failed claim scans, full topic/ACL/schema orphan, and explicit partial cleanup.
  Assert one cumulative deadline across multiple pages and Kafka reads, no retries or
  backoff, cancellation, and ACL-lock release on timeout/error.
- CRD/CEL: `topicName` omitted-to-same-explicit accepted; omitted-to-different rejected;
  explicit-to-different rejected; all tested via API server with webhooks disabled. Verify the
  effective-name expression is emitted in generated CRD schema (not only Go markers).

### End-to-end profile

- Add `cluster-link` profile: two single-node `confluentinc/cp-server` 8.x clusters (`source`,
  `dest`) under Compose, with a link initialized using `kafka-cluster-links`. Add a
  `cluster-linking` capability tag to `internal/matrix/versions.yaml`; unsupported matrix cells
  skip the scenario with an explicit reason.
- Add scenario `26-mirror-topic`: create, source produce/destination consume, mirror pause drift,
  healthy mirror verification, import, and collision with an existing normal topic.
- Extend the scenario or integration suite with the following safety cases:

| Scenario | Required outcome |
|---|---|
| REST list timeout or `403` during `KafkaTopic` guard | No unverified topic mutation; explicit state-read error |
| Kafka Admin ID A, configured/discovered REST ID B | `ClusterIdentityMismatch`, no create/alter/delete or mixed snapshot |
| Two namespace-local `KafkaCluster/dr` CRs in different namespaces | Same topic name is *not* a duplicate claim if references resolve to distinct cluster CRs |
| Two different namespaces resolve to one global `KafkaCluster/dr` | Cross-kind same-topic collision; one deterministic winner, with shared lock and quorum recheck |
| Webhooks disabled, omitted `topicName` updated to a different explicit name | API server CEL rejects update; same effective name is allowed |
| Paginated mirror response | Complete inventory; no false normal-topic classification |
| Prefixed link with incorrect or omitted destination `topicName` | Reject create |
| Correct name but mirror on different link/source | Reject rebind |
| Mirror `FAILED`, declared config still matches | `verify` detects lifecycle-only drift |
| Restore variant state | Recognized as progressing (until stalled) |
| `promote` -> `STOPPED` -> `import` | Import as `KafkaTopic` when no active mirror claim remains |
| `STOPPED` with active `KafkaMirrorTopic` claim during import | Explicit migration conflict; no competing `KafkaTopic` silently adopted |
| `PENDING_STOPPED` during import | Deferred, not adopted |
| Delete a mirror CR (including while promotion occurs) | Mirror/normal topic remains intact |
| Delete a `KafkaTopic` CR with policy `Delete`, no active co-claimant, and live `ACTIVE` mirror | Initial classification blocks before ACL/topic/schema cleanup even with allow-delete |
| Delete a `KafkaTopic` CR while an active co-claimant protects the same topic | Full orphan: no topic/ACL/schema cleanup, warning naming claimant, finalizer removed; a retained normal topic still prevents mirror create |
| REST endpoint slow during the `KafkaTopic` finalizer | Preparation occurs outside ACL lock; final verification has one total deadline, no retries, and releases the lock on timeout; other ACL writers can wait but then continue |
| Initial KafkaTopic deletion guard REST timeout/`403`/cluster mismatch, no co-claimant | Finalizer retains; no destructive cleanup starts; force removal may orphan only |
| Live mirror changes state between initial delete guard and actual `DeleteTopic` | Final all-links verification blocks if observable; any earlier ACL cleanup is reported; concurrent post-check TOCTOU remains documented |
| Final verification of a normal topic with no known `linkName` | Uses fresh complete all-links mirror index plus Kafka Admin evidence; no invented wildcard describe |
| Topic replaced by a mirror on a different link before final verification | Matching entry anywhere in the fresh index prevents `DeleteTopic` |
| A later REST page fails, repeats a next link, or exceeds the deadline | `Unknown`, no delete or further cleanup; no partial index is accepted |
| Multiple individually fast pages exceed the cumulative verification budget | Entire attempt fails on the original deadline, not a per-page reset; lock is released |
| Final verification receives `429` with `Retry-After` | No sleep/retry under ACL lock; retain finalizer and requeue after releasing the lock |
| Positive co-claimant plus live `Mirror` or `StoppingMirror` | Full orphan before cluster reads; no destructive calls; finalizer removed |
| Positive co-claimant plus unavailable REST or invalid destination credentials | Full orphan does not call Kafka/REST and is not blocked by their availability |
| Co-claimant scan cannot confirm a complete negative result | No cleanup or assumed absence of claims; finalizer retains |
| Crash/restart after successful `DeleteTopic`, before finalizer removal | Fresh `Absent` skips topic deletion; remaining approved ownership-safe cleanup can finish |
| Preflight and final verification both return `Absent` | No `DeleteTopic`; no recreation; remove finalizer after remaining required cleanup completes |
| Preflight `Absent`, but final verification finds a topic now exists | Abort absence-recovery attempt and requeue for fresh preflight; no unplanned deletion |
| Topic absent in Kafka, but REST has contradictory mirror evidence | Preserve `Unknown`; absence recovery does not weaken the shared classifier |
| Final verification fails after authorized ACL cleanup | No `DeleteTopic` or subsequent schema cleanup; explicit partial-cleanup diagnostic; finalizer retains |
| Full orphan after a prior partial-cleanup attempt | No additional cleanup or compensating recreation; retained resources are not described as restored |
| Another ACL writer waits while final verification times out | Added wait is bounded by the verification deadline; cancellation releases the lock and the writer proceeds |
| Both kinds claim the same topic (including webhook race and equal `namespace/name`) | Exactly one deterministic winner per the extended tie-break; the other is blocked |
| `KafkaMirrorTopic` with `deletionPolicy: Delete` (CLI and operator) | Rejected by validation/admission, not silently orphaned |
| RF mismatch on existing mirror | Visible non-executable drift |
| POST create times out after server-side success | Next run observes the mirror; no blind duplicate create |
| `apply` plan says `Absent` but a normal topic appears before write | Targeted pre-write read rejects create, no overwrite |
| REST credentials Secret rotates while operator is running | Reconstructed client uses new credentials, no stale authentication |
| Link config unreadable | No create/update based on an assumed empty prefix |
| ACL sync enabled | No automatic GitOps adoption of ambiguous synced ACLs |

### Version and cloud compatibility

- Integration-test the REST implementation against real `cp-server`; verify exact REST response
  fields and supported status values across the versions selected for the matrix.
- The minimum supported Confluent Platform version and matrix coverage remain a **release-gate
  decision**, not merely an implementation detail. Confirm which REST fields and link configs are
  available before enabling the scenario for a version.
- Confluent Cloud remains covered by the opt-in cloud harness, not default CI. It must cover
  explicit `clusterId`, auth, link config reads, and a mirror creation/describe smoke test.
- Finalizer release gate: exercise the complete all-links verification against each supported
  platform/version, including a normal topic with no known link, multiple pages, historical
  `STOPPED`, and replacement by a mirror on another link. Validate the proposed timeout at
  representative inventory sizes and document tuning/Orphan fallback. A cluster too large
  to verify within its configured budget remains protected; never weaken completeness to
  make deletion pass. An optimized lookup needs its own equivalent-coverage proof and tests.

## Implementation plan and effort estimate

**Phase 1 — CLI vertical slice:** REST client, link config checks, REST/Admin cluster-ID
binding, complete snapshots, classification, resolved identity, diff findings, mirror
create/config update with payload mapping and just-in-time pre-write reads, KafkaTopic guard,
verify, conservative import, doctor. No mirror deletion.

**Phase 2 — Operator:** CRD + spec-scoped effective-name CEL, webhook, resolved-cluster
cross-kind duplicate claim resolution, shared locks/quorum checks, guarded existing KafkaTopic
`Delete` finalizer with authoritative all-links verification, full-orphan precedence,
absence recovery and a whole-attempt deadline, shared reconcile, orphan-on-mirror-CR-delete,
Secret-rotation wiring, status/conditions, metrics, Helm/RBAC, controller tests.

**Phase 3 — Hardening:** real cp-server two-cluster e2e; REST failure and pagination cases;
compatibility matrix; optional Confluent Cloud smoke test.

Revised planning estimate: **4–6 weeks for one developer familiar with the codebase**, subject to
repository integration and cp-server CI startup/license constraints. This is a planning range,
not a measured implementation commitment. The original v1's 3.5–4 weeks did not fully budget
for shared ownership/classification, error paths, and safety tests. CLI-only may still be a
useful first milestone; exact LOC estimates should be recalculated after the shared snapshot
interface is prototyped.

## Decisions locked for v1

- Separate `KafkaMirrorTopic` kind; existing cluster links only.
- Confluent Kafka REST v3 for mirror and link operations; Kafka Admin for common topic metadata.
- One shared topic classification and identity space across both kinds; operator claim keys
  include **resolved KafkaCluster namespace/name**, not just `clusterRef.name`, and locks are
  shared across kinds.
- Fail closed when configured REST classification is incomplete/unavailable or when Kafka Admin
  and REST identify different clusters; refresh classification before writes.
- No implicit mirror lifecycle management.
- No mirror deletion in either CLI or operator; CR deletion always orphans. `deletionPolicy`
  accepts only `Orphan`.
- Duplicate claims use the existing timestamp → `namespace/name` order, extended with kind.
- `replicationFactor` is create-time only; mismatches are reported.
- `topicName` resolves offline; prefixed links require explicit destination naming and live
  prefix validation. Spec-scoped CRD CEL enforces immutability of the effective name even without
  webhooks.
- `STOPPED` is importable as `KafkaTopic`; `PENDING_STOPPED` is not auto-adopted.
- Lifecycle-only drift is reported even when no executable operation exists.
- Mirrored config/ACL provenance is not silently adopted by import.
- A legacy `KafkaTopic` with `deletionPolicy: Delete` has a mirror-aware, fail-closed finalizer
  guard before destructive cleanup; a read/mismatch failure is not delete permission.
- A positive active co-claimant at `KafkaTopic` deletion means full skip-and-orphan: no further
  topic, ACL, or schema cleanup; warn and remove the finalizer before Kafka/REST checks.
  Failed claim scans never authorize destructive work.
- Confirmed `Absent` completes the topic-deletion step; remaining cleanup retains all policy
  and shared-ownership gates. Restarting after a successful delete must be idempotent.
- The final authoritative check combines fresh Kafka Admin evidence with a complete all-links
  mirror index. A single-link describe or partial page set is not sufficient negative proof.
- The `KafkaTopic` finalizer takes no identity lock. Preparation stays outside the ACL lock;
  final verification under it has one total finite budget (proposed default 5 seconds), no
  retries or backoff, and guaranteed cancellation/lock release on failure. Other ACL writers
  may wait during this budget; no zero-latency-impact promise is made.
- A late guard failure stops all further cleanup and explicitly reports any known partial
  cleanup. Neither a later orphan nor a retry silently recreates already removed resources.
- REST create payloads serialize config entries in the endpoint's `{name, value}` representation.
- The `KafkaTopic` co-claimant scan reuses the existing age-blind User/Quota deletion-scan pattern
  and runs before destination clients are built; the ACL co-ownership shield remains on the
  no-claimant path.

## Open implementation details and release gates

These do not change the conceptual contract above:

1. Exact Go package/type names and whether findings extend `internal/operations` or use a sibling
   type; both CLI and operator must consume the same findings.
2. Status condition codes and metrics naming, aligned with existing operator conventions.
3. Default configuration plumbing for the 30-minute stalled-transition threshold.
4. `clusterId` caching boundaries and snapshot reuse within operator reconciliation batches.
5. Minimum Confluent Platform release and tested REST-status compatibility matrix, including
   the exact mirror-create payload schema supported by the selected versions.
6. How import warnings/partial state map to existing importer exit status and output format,
   without silently presenting an incomplete import as lossless.
7. Behavior when effective synced-config ownership cannot be determined for an explicitly
   declared config key: choose a stable error code, with safety preferred over speculative alter.
8. Operational warning/documentation for legacy `KafkaTopic` delete TOCTOU risk, even with fresh
   reads; no new atomic delete primitive is assumed. Include concurrent claim creation and
   partial-cleanup outcomes, not only mirror lifecycle changes.
9. Exact operator flag/config plumbing for the finite final-verification budget (v6 proposes
   a 5-second default), plus representative pagination/load and cancellation tests. The
   one-deadline/no-retry/complete-index semantics are fixed; naming and deployment tuning are
   implementation details. An optimization cannot replace the baseline without equivalent
   authoritative cross-link coverage on each supported version.

## References

- Repository: https://github.com/monedula-dev/monedula-gitops
- Existing duplicate gate, scan scope and timestamp/name ordering:
  https://github.com/monedula-dev/monedula-gitops/blob/main/internal/operator/controller/duplicate.go
- Existing KafkaTopic cluster resolution, locks, and deletion finalizer:
  https://github.com/monedula-dev/monedula-gitops/blob/main/internal/operator/controller/kafkatopic_controller.go
- Confluent Platform REST API (v3, cluster links and mirrors):
  https://docs.confluent.io/platform/current/kafka-rest/api.html
- Confluent Platform mirror topic lifecycle and prefix behavior:
  https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/mirror-topics-cp.html
- Confluent Cloud mirror lifecycle, including historical `STOPPED` mirror listings:
  https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/mirror-topics-cc.html
- Confluent Platform Cluster Linking security requirements:
  https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/security.html
- Confluent Cloud REST schema for mirror-status values:
  https://docs.confluent.io/cloud/current/ccloud/list-kafka-mirror-topics/
