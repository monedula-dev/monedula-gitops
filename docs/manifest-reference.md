# Manifest Reference

Reference for the Monedula GitOps resource kinds. Every field in the
tables links to its description — click through to navigate.

All resources use:

```yaml
apiVersion: gitops.monedula.dev/v1alpha1
```

| Kind | Purpose |
| ---- | ------- |
| [`KafkaCluster`](#kafkacluster) | Connection details and defaults for a Kafka cluster |
| [`KafkaTopic`](#kafkatopic) | One topic: lifecycle, config, topic-local access, schema |
| [`KafkaAccessPolicy`](#kafkaaccesspolicy) | Advanced / shared / raw Kafka ACL rules |
| [`KafkaQuota`](#kafkaquota) | Per user / client-id Kafka quota limits |
| [`KafkaRoleBinding`](#kafkarolebinding) | Confluent RBAC role binding via MDS (CLI + operator, v0.13/v0.14) |
| [`KafkaUser`](#kafkauser) | A Kafka SCRAM principal — username + mechanism + password (CLI + operator, v0.35) |

Shared building blocks: [`valueFrom`](#valuefrom) · [Annotations](#annotations) ·
[Reconciliation modes](#reconciliation-modes) · [Deletion policies](#deletion-policies) ·
[ACL pruning](#pruning) · [RBAC pruning](#rbac-pruning)

---

<a id="kafkacluster"></a>
## KafkaCluster

Defines how Monedula connects to a Kafka cluster (and optionally its Schema
Registry). In CLI mode it is loaded via `--cluster-config-file`; in Kubernetes
mode it is a CRD watched by the operator.

```yaml
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaCluster
metadata:
  name: prod-eu
spec:
  bootstrapServers: "b1:9093,b2:9093"
  tls:
    enabled: true
  auth:
    mechanism: SCRAM-SHA-512
    scram:
      username: { valueFrom: { env: KAFKA_USERNAME } }
      password: { valueFrom: { env: KAFKA_PASSWORD } }
  schemaRegistry:
    endpoint: "http://sr:8081"
    auth:
      type: basic
      username: { valueFrom: { env: SR_USERNAME } }
      password: { valueFrom: { env: SR_PASSWORD } }
  defaults:
    replicationFactor: 3
```

<a id="kafkacluster-spec"></a>
### KafkaCluster `spec`

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| [`bootstrapServers`](#kafkacluster-bootstrapservers) | string | **yes** | Comma-separated broker list |
| [`tls`](#kafkacluster-tls) | object | no | TLS settings |
| [`auth`](#kafkacluster-auth) | object | no | SASL authentication |
| [`schemaRegistry`](#kafkacluster-schemaregistry) | object | no | Schema Registry connection |
| [`defaults`](#kafkacluster-defaults) | object | no | Cluster-level topic defaults |
| [`tenancy`](#kafkacluster-tenancy) | object | no | Namespace allow-lists + topic-prefix policy (operator-enforced) |
| [`authorization`](#kafkacluster-authorization) | object | no | Authorization backends beyond ACLs (MDS/RBAC) |

<a id="kafkacluster-bootstrapservers"></a>
#### `spec.bootstrapServers`

Comma-separated `host:port` broker list, e.g. `"b1:9093,b2:9093"`. Required.

<a id="kafkacluster-tls"></a>
#### `spec.tls`

| Field | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `enabled` | bool | `false` | Enable TLS for the broker connection (system trust roots) |
| `caCert` | [`valueFrom`](#valuefrom) | — | Custom CA certificate PEM (or bundle) used to verify the broker. Resolves like any other secret value: `file`/`env`/`inline` in CLI mode, `secretKeyRef` in operator mode |
| `clientCert` | [`valueFrom`](#valuefrom) | — | TLS client certificate (PEM) for [`mTLS`](#kafkacluster-auth) auth. Must be set together with `clientKey` and requires `enabled: true` |
| `clientKey` | [`valueFrom`](#valuefrom) | — | TLS client private key (PEM). Must be set together with `clientCert` |
| `insecureSkipVerify` | bool | `false` | Skip server certificate verification. Dev-only escape hatch — prefer `caCert` for private CAs |

<a id="kafkacluster-auth"></a>
#### `spec.auth`

| Field | Type | Description |
| ----- | ---- | ----------- |
| `mechanism` | string | One of `None`, `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, `mTLS` |
| `scram.username` | [`valueFrom`](#valuefrom) | SASL username. The `scram` block supplies credentials for **both** SCRAM and `PLAIN` |
| `scram.password` | [`valueFrom`](#valuefrom) | SASL password |
| `oauth.tokenEndpoint` | string | OIDC token endpoint URL (`OAUTHBEARER` only). Required for `OAUTHBEARER` |
| `oauth.clientId` | [`valueFrom`](#valuefrom) | OIDC client id (`OAUTHBEARER` only) |
| `oauth.clientSecret` | [`valueFrom`](#valuefrom) | OIDC client secret (`OAUTHBEARER` only) |
| `oauth.scope` | string | Optional OAuth scope (`OAUTHBEARER` only) |
| `oauth.tokenEndpointCA` | [`valueFrom`](#valuefrom) | Optional PEM CA (bundle) to trust for the IdP's `tokenEndpoint` TLS connection. The IdP is a **different trust domain** than the Kafka brokers — this is never derived from or shared with [`tls.caCert`](#kafkacluster-tls). Omit to use the system trust store |

`OAUTHBEARER` uses the OIDC client-credentials flow (`oauth.*`); `auth.scram`
must be absent. `mTLS` authenticates with a TLS client certificate: it requires
`tls.enabled: true` plus [`tls.clientCert`](#kafkacluster-tls) / `tls.clientKey`,
and `auth.scram` / `auth.oauth` must be absent.

<a id="kafkacluster-schemaregistry"></a>
#### `spec.schemaRegistry`

Optional. Required if any [`KafkaTopic`](#kafkatopic) referencing this cluster
sets [`spec.schema`](#kafkatopic-schema).

| Field | Type | Description |
| ----- | ---- | ----------- |
| `endpoint` | string | Schema Registry base URL, e.g. `http://sr:8081`. Required when the block is present |
| `auth.type` | string | `basic` (HTTP Basic auth) |
| `auth.username` | [`valueFrom`](#valuefrom) | Basic-auth username |
| `auth.password` | [`valueFrom`](#valuefrom) | Basic-auth password |
| `tls` | object | TLS settings for the Schema Registry HTTPS connection (same shape as [`spec.tls`](#kafkacluster-tls)): `caCert` for a private CA, `clientCert`/`clientKey` for a TLS client certificate, `insecureSkipVerify` as a dev-only escape hatch |

<a id="kafkacluster-defaults"></a>
#### `spec.defaults`

Applied to topics that omit the corresponding value.

| Field | Type | Description |
| ----- | ---- | ----------- |
| `replicationFactor` | int | Default replication factor for topics that omit [`spec.replicationFactor`](#kafkatopic-replicationfactor). **Skipped** for topics that configure `confluent.placement.constraints` (mutually exclusive — see [`replicationFactor`](#kafkatopic-replicationfactor)) |
| `topicDeletionPolicy` | string | `Orphan` \| `Delete`. Applied as the default [`deletionPolicy`](#deletion-policies) for topics that omit `spec.deletionPolicy` |

<a id="kafkacluster-tenancy"></a>
#### `spec.tenancy`

Multi-tenancy policy, **enforced by the operator** (terminal `TenancyDenied`);
the CLI only shape-validates it.

```yaml
tenancy:
  allowedNamespaces: [team-payments, team-payments-*]
  topicPrefixes:
    - namespaces: [team-payments, team-payments-*]
      prefixes:   [payments.]
```

| Field | Type | Description |
| ----- | ---- | ----------- |
| `allowedNamespaces` | list\<string\> | Glob patterns (`path.Match`); applies to ALL data-plane kinds (`KafkaTopic`, `KafkaAccessPolicy`, `KafkaQuota`, `KafkaRoleBinding`) — a resource whose namespace matches none is rejected. Empty = any namespace |
| `topicPrefixes[].namespaces` | list\<string\> | Glob patterns selecting the namespaces this rule applies to (non-empty); matching namespaces are *prefix-restricted* |
| `topicPrefixes[].prefixes` | list\<string\> | Allowed name prefixes for the selected namespaces (non-empty). Checked against resolved `topicName`s, topic/group policy-rule resources, consumer group names in topic access blocks, and `Topic`/`Group` role-binding resources (groups reuse topic prefixes). Prefix-restricted namespaces are denied unscopeable grants: policy rules on `cluster`/`transactionalId`/`delegationToken` resources, cluster-scoped role bindings, and role-binding resources of other types. See [operator docs](operator.md#tenancy) |

<a id="kafkacluster-authorization"></a>
#### `spec.authorization`

Optional. Configures authorization backends beyond Kafka ACLs. Currently holds the MDS connection for Confluent RBAC.

| Field | Type | Description |
| ----- | ---- | ----------- |
| [`mds`](#kafkacluster-authorization-mds) | object | Confluent Metadata Service (RBAC) connection |

<a id="kafkacluster-authorization-mds"></a>
#### `spec.authorization.mds`

Confluent MDS connection. Required when any [`KafkaRoleBinding`](#kafkarolebinding) references this cluster.

```yaml
authorization:
  mds:
    endpoint: "https://mds.example.com:8090"
    auth:
      type: basic
      username: { valueFrom: { env: MDS_USERNAME } }
      password: { valueFrom: { env: MDS_PASSWORD } }
    clusters:
      kafkaCluster: "abc123"
      schemaRegistryCluster: "sr-abc"   # required for scope.type: schema-registry
      connectCluster: "connect-1"       # required for scope.type: connect
      ksqlCluster: "ksql-1"             # required for scope.type: ksql
```

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `endpoint` | string | **yes** | MDS base URL, e.g. `https://mds.example.com:8090` |
| `auth` | object | no | MDS authentication credentials (see below) |
| `tls` | object | no | TLS settings for the MDS connection (same shape as [`spec.tls`](#kafkacluster-tls)) |
| `clusters.kafkaCluster` | string | **yes** | Kafka cluster ID — required on every MDS scope |
| `clusters.schemaRegistryCluster` | string | no | Schema Registry cluster ID — required when any binding uses `scope.type: schema-registry` |
| `clusters.connectCluster` | string | no | Kafka Connect cluster ID — required when any binding uses `scope.type: connect` |
| `clusters.ksqlCluster` | string | no | ksqlDB cluster ID — required when any binding uses `scope.type: ksql` |

**`spec.authorization.mds.auth`** — authentication for the MDS REST API. `type` selects the method:

| `type` | Field(s) | Description |
| ------ | -------- | ----------- |
| `basic` | `username`, `password` ([`valueFrom`](#valuefrom)) | HTTP Basic auth |
| `bearer` | `token` ([`valueFrom`](#valuefrom)) | Bearer token |
| `mtls` | — | TLS client certificate from `authorization.mds.tls.clientCert` / `clientKey` |

> **Coexistence with ACLs.** `authorization.mds` is independent of `spec.auth` (Kafka broker SASL). You may configure both — the broker connection uses `spec.auth`; the MDS connection uses `authorization.mds.auth`. `KafkaAccessPolicy` and `KafkaTopic.spec.access` ACLs are unaffected by the presence of an MDS config.

> **`accessBackends` is deferred to v0.15.** The optional `authorization.accessBackends` list (`[acl]`/`[rbac]`/`[acl, rbac]`) — which governs whether `KafkaTopic.spec.access` producers/consumers emit ACLs, RBAC role bindings, or both — is not yet implemented. v0.13 adds only `authorization.mds`; the topic-access auto-map lands in v0.15.

---

<a id="kafkatopic"></a>
## KafkaTopic

The primary application-facing resource: one Kafka topic plus its config,
topic-local producer/consumer access, and Schema Registry subjects.

```yaml
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaTopic
metadata:
  name: orders
  namespace: payments
spec:
  clusterRef:
    name: prod-eu
  topicName: payments.orders
  partitions: 6
  config:
    retention.ms: "604800000"
  access:
    producers:
      - principal: User:svc-checkout
    consumers:
      - principal: User:svc-fraud
        group: fraud-orders
  schema:
    format: AVRO
    compatibility: BACKWARD
    valueSchema: { valueFrom: { file: ./schemas/orders-value.avsc } }
  reconciliation:
    mode: Enforce
  deletionPolicy: Orphan
```

<a id="kafkatopic-spec"></a>
### KafkaTopic `spec`

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| [`clusterRef`](#kafkatopic-clusterref) | object | **yes** | The [`KafkaCluster`](#kafkacluster) this topic lives on. **Immutable** (always-on CRD validation rule) |
| [`topicName`](#kafkatopic-topicname) | string | no | Actual Kafka topic name (defaults to `metadata.name`). **Immutable once set** (always-on CRD validation rule) |
| [`partitions`](#kafkatopic-partitions) | int | **yes** | Partition count (≥ 1) |
| [`replicationFactor`](#kafkatopic-replicationfactor) | int | no | Replication factor — omit to use the broker default or placement constraints |
| [`config`](#kafkatopic-config) | map | no | Topic configuration (only declared keys are managed) |
| [`access`](#kafkatopic-access) | object | no | Topic-local producer/consumer access |
| [`schema`](#kafkatopic-schema) | object | no | Schema Registry subjects for this topic |
| [`reconciliation`](#reconciliation-modes) | object | no | `mode`: `Enforce` (default) \| `DetectOnly` \| `ObserveOnly` |
| [`drift`](#kafkatopic-drift) | object | no | Drift-calculation tuning |
| [`prune`](#pruning) | bool | no | Opt in to ACL pruning for this resource's scope (operator mode). Default `false` |
| [`deletionPolicy`](#deletion-policies) | string | no | `Orphan` (default) \| `Delete` |

<a id="kafkatopic-clusterref"></a>
#### `spec.clusterRef`

```yaml
clusterRef:
  name: prod-eu
```

References a [`KafkaCluster`](#kafkacluster) by name. In CLI mode the cluster
must be loaded via `--cluster-config-file`; an unknown reference is an error.
In operator mode the `KafkaCluster` CR is looked up in the resource's namespace
(or the operator's `--cluster-namespace`).

<a id="kafkatopic-topicname"></a>
#### `spec.topicName`

The actual Kafka topic name. Defaults to `metadata.name`. **Immutable once
set** — enforced by the Kubernetes apiserver itself via a CRD validation rule
(`x-kubernetes-validations`), so it holds even in the default install with
webhooks disabled. An update that sets a previously-unset `topicName` is
allowed (making the `metadata.name` default explicit); once set, changing or
removing it is rejected (a rename is a delete + create of a different Kafka
topic, silently orphaning the old one). `spec.clusterRef.name` is immutable the
same way. Topic identity is `(cluster, topicName)` — two loaded topics
resolving to the same identity are a validation error. `metadata.namespace` is
organizational only (it is *not* part of the Kafka identity).

<a id="kafkatopic-partitions"></a>
#### `spec.partitions`

Must be ≥ 1. Increases are applied but **gated** by
[`allow-destructive`](#annotations). Decreases are **always rejected** (Kafka
cannot shrink a topic).

<a id="kafkatopic-replicationfactor"></a>
#### `spec.replicationFactor`

Optional; if set, must be ≥ 1.

- **Omitted**: falls back to the cluster's
  [`defaults.replicationFactor`](#kafkacluster-defaults) if set; otherwise the
  create request sends `-1` and the **broker default**
  (`default.replication.factor`) applies.
- **Mutually exclusive with `confluent.placement.constraints`**: if that key is
  present in [`config`](#kafkatopic-config), setting `replicationFactor` is a
  validation error, and the cluster-level default is *not* injected — the
  placement constraint determines replication.
- Changing the replication factor of an existing topic is **Blocked** without
  [`allow-destructive`](#annotations); even with the gate, apply reports it as
  `Unsupported` — it cannot perform the required partition reassignment.

<a id="kafkatopic-config"></a>
#### `spec.config`

String-to-string map of topic configs:

```yaml
config:
  retention.ms: "604800000"
  cleanup.policy: delete
```

**Only declared keys are managed**: live config keys not listed here are never
touched and never reported as drift. Values must be strings (quote numbers).

<a id="kafkatopic-access"></a>
#### `spec.access`

Intentionally high-level, topic-local access. Patterns are always literal. An
optional per-entry `host` field (default `*`) restricts the ACL(s) to a source
host (spec §8.4); for prefixed, shared-group, or other advanced rules use a
[`KafkaAccessPolicy`](#kafkaaccesspolicy).

| Field | Type | Description |
| ----- | ---- | ----------- |
| [`producers`](#kafkatopic-producers) | list | Principals allowed to produce to this topic |
| [`consumers`](#kafkatopic-consumers) | list | Principals allowed to consume from this topic |

<a id="kafkatopic-producers"></a>
##### `access.producers[]`

| Field | Required | Default | Description |
| ----- | -------- | ------- | ----------- |
| `principal` | **yes** | — | e.g. `User:svc-checkout` |
| `host` | no | `*` | Restrict ACL(s) to a source host or CIDR (spec §8.4). Blank is rejected; omit for all-hosts. |
| `operations` | no | `[Write, Describe]` | Topic operations |

Compiles to literal topic ACLs on [`topicName`](#kafkatopic-topicname)
(`Allow`, host from entry or `*`).

<a id="kafkatopic-consumers"></a>
##### `access.consumers[]`

| Field | Required | Default | Description |
| ----- | -------- | ------- | ----------- |
| `principal` | **yes** | — | e.g. `User:svc-fraud` |
| `group` | **yes** | — | Consumer group this principal reads with |
| `host` | no | `*` | Restrict topic + group ACL(s) to a source host or CIDR (spec §8.4). Blank is rejected; omit for all-hosts. |
| `topicOperations` | no | `[Read, Describe]` | Topic operations |
| `groupOperations` | no | `[Read]` | Group operations |

Compiles to a literal topic ACL **and** a group ACL (`Allow`, host from entry
or `*`).

<a id="kafkatopic-schema"></a>
#### `spec.schema`

Schema Registry subjects for this topic. Requires the referenced cluster to
configure [`schemaRegistry`](#kafkacluster-schemaregistry).

| Field | Required | Description |
| ----- | -------- | ----------- |
| `format` | **yes** | `AVRO` \| `JSON` \| `PROTOBUF` |
| `subjectStrategy` | no | `TopicName` (default; subjects `<topicName>-value`/`-key`), `RecordName` (record full name), `TopicRecordName` (`<topicName>-<recordFullName>`), or `Custom` (verbatim via `valueSubject`/`keySubject`) |
| `compatibility` | no\* | `NONE`, `BACKWARD`, `BACKWARD_TRANSITIVE`, `FORWARD`, `FORWARD_TRANSITIVE`, `FULL`, `FULL_TRANSITIVE`. Raising is ungated; **lowering (or sideways) is gated** by [`allow-destructive`](#annotations). A **first-time** subject-level set is classified against the registry's **global default** (the subject's effective level): declaring a level below it is a gated lowering too. \*Required in governance mode (no `valueSchema`) |
| `valueSchema` | no | [`valueFrom`](#valuefrom) ref to the value schema body. `file:` paths resolve relative to this manifest. **Omit for governance mode** (see below). In operator mode use `inline` / `configMapKeyRef` (file refs unsupported) |
| `keySchema` | no | Same, for the key subject |
| `valueSubject` | no | Value subject name, verbatim. Required when `subjectStrategy: Custom` |
| `keySubject` | no | Key subject name, verbatim (`Custom` only; required if a `keySchema` is set) |

**Modes.** *Content mode* sets `valueSchema` — Monedula registers and manages
the schema body. *Governance mode* omits `valueSchema`/`keySchema` (only `format`
+ `compatibility`): Monedula manages **only** the subject's compatibility level
and never registers a version — producer pipelines own the content. Removing the
whole `schema` block **orphans** the subjects (they are never auto-deleted); a
topic delete (`deletionPolicy: Delete` + [`allow-delete`](#annotations)) deletes
content-mode subjects but never governance-mode ones.

**Schema import reconstruction.** `import cluster` reconstructs schema subjects
onto topics by naming convention:

- **TopicName** (`<topic>-value` / `<topic>-key`): folded into `spec.schema` on the owning topic.
- **TopicRecordName** (`<topic>-<recordFullName>`): reconstructed onto the owning topic with
  `subjectStrategy: TopicRecordName`. The record full name is extracted from the schema body and the
  match is body-verified. A record-based subject is assigned to the value schema by default (key
  subjects are rare); if two record-based subjects map to one topic, one is used as the value and
  the ambiguity is reported in the import summary.
- **RecordName** (subject name == record full name, no topic prefix): not attributed to any topic.
  These appear in a dedicated "RecordName subjects needing manual attribution" report section (subject
  name, record name, schema type) and are never written into a manifest, because the topic↔schema
  link is producer-side config that is absent from cluster state and the schema may be shared.
- **Custom / hand-named subjects**: remain in the generic unmatched-subjects warning.

Pass `--skip-schemas` to skip schema reconstruction entirely — no Schema Registry connection is
made, no `spec.schema` is emitted, and the RecordName/unmatched report is suppressed. Use this
when schemas are owned by producer applications or CI pipelines and should not be imported.

<a id="kafkatopic-drift"></a>
#### `spec.drift`

| Field | Description |
| ----- | ----------- |
| `ignoreFields` | List of field paths to exclude from drift calculation in `verify`, `diff`, and `apply`. Valid entries: `partitions`, `replicationFactor`, `config.<key>` (e.g. `config.segment.bytes`); anything else is a validation error |

---

<a id="kafkaaccesspolicy"></a>
## KafkaAccessPolicy

Raw Kafka ACL rules for everything that is not naturally owned by exactly one
topic: prefixed patterns, cluster/transactional-ID/delegation-token resources,
`Deny` rules, host-restricted rules, shared groups.

```yaml
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaAccessPolicy
metadata:
  name: billing-access
spec:
  clusterRef:
    name: prod-eu
  rules:
    - principal: User:svc-billing
      host: "10.0.0.15"
      resource:
        type: topic
        name: billing.
        patternType: prefixed
      operations: [Read, Write, Describe]
  deletionPolicy: Orphan
```

<a id="kafkaaccesspolicy-spec"></a>
### KafkaAccessPolicy `spec`

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| [`clusterRef`](#kafkatopic-clusterref) | object | **yes** | The [`KafkaCluster`](#kafkacluster) these ACLs live on. **Immutable** (always-on CRD validation rule) |
| [`rules`](#kafkaaccesspolicy-rules) | list | **yes** (non-empty) | The ACL rules |
| [`reconciliation`](#reconciliation-modes) | object | no | `mode`: `Enforce` (default) \| `DetectOnly` \| `ObserveOnly` |
| [`prune`](#pruning) | bool | no | Opt in to ACL pruning for this resource's scope (operator mode). Default `false` |
| [`deletionPolicy`](#deletion-policies) | string | no | `Delete` (default for policies) \| `Orphan` |

<a id="kafkaaccesspolicy-rules"></a>
#### `spec.rules[]`

| Field | Required | Default | Description |
| ----- | -------- | ------- | ----------- |
| `principal` | **yes** | — | e.g. `User:svc-billing` |
| `permission` | no | `Allow` | `Allow` \| `Deny` |
| `host` | no | `*` | Restrict the rule to a client host (Kafka matches the client IP as seen by the broker) |
| `resource.type` | **yes** | — | `topic` \| `group` \| `cluster` \| `transactionalId` \| `delegationToken` |
| `resource.name` | **yes** | — | Resource name (or prefix when `patternType: prefixed`) |
| `resource.patternType` | no | `literal` | `literal` \| `prefixed` |
| `operations` | **yes** (≥ 1) | — | Any of `Read`, `Write`, `Create`, `Delete`, `Alter`, `Describe`, `ClusterAction`, `DescribeConfigs`, `AlterConfigs`, `IdempotentWrite`, `All` |

Requesting the same ACL tuple as both `Allow` and `Deny` (across any loaded
manifests) is a validation error. Cross-resource Allow/Deny conflicts — the same
`(principal, host, resource, operation)` granted `Allow` by one resource and
`Deny` by another (including `KafkaTopic` topic-local access vs. a
`KafkaAccessPolicy` rule) — are handled in three complementary ways:

* **CLI (`validate` / `apply`):** caught at load time; exits 2 with a conflict
  message naming the tuple.
* **Operator — admission webhook** (`--enable-webhooks`): the
  `KafkaAccessPolicy` validating webhook rejects a conflicting policy at
  admission time, naming the opposing resource and the contested tuple.
* **Operator — reconcile condition** (always-on): the reconciler sets an
  `ACLConflict` status condition (`True`) on every resource party to the
  conflict, regardless of whether the webhook is enabled. The conflicting tuple
  is dropped from the applied union so reconciliation does not flap; the
  conflict is reported, not silently swallowed.

ACLs no longer desired are **pruned**, but only within the managed scope
(principals + resource patterns your manifests reference) and only when pruning
is [opted in](#pruning) (`apply --prune` / `spec.prune`).

---

<a id="kafkaquota"></a>
## KafkaQuota

Declaratively limits a Kafka `user` principal and/or `client-id`. One CR owns
one quota entity 1:1. Setting or updating a limit is **ungated** (`RiskLow` —
fully reversible, no data loss), but **removing** a live limit key
(`RemoveQuota`) unthrottles a client and is gated by
[`allow-destructive`](#annotations) (`RiskMedium`).

```yaml
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaQuota
metadata:
  name: svc-checkout-limit
spec:
  clusterRef:
    name: prod-eu
  entity:
    user: User:svc-checkout        # "User:" form (stripped for the quota API)
    clientId: batch                # combine with user for the (user+client-id) entity
  limits:
    producerByteRate: 1048576      # bytes/sec
    consumerByteRate: 2097152      # bytes/sec
  reconciliation:
    mode: Enforce
```

<a id="kafkaquota-spec"></a>
### KafkaQuota `spec`

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| [`clusterRef`](#kafkatopic-clusterref) | object | **yes** | The [`KafkaCluster`](#kafkacluster) this quota lives on. **Immutable** (always-on CRD validation rule) |
| [`entity`](#kafkaquota-entity) | object | **yes** | Quota target — at least one component. **Immutable** (always-on CRD validation rule) |
| [`limits`](#kafkaquota-limits) | object | **yes** | Quota values — at least one |
| [`reconciliation`](#reconciliation-modes) | object | no | `mode`: `Enforce` (default) \| `DetectOnly` \| `ObserveOnly` |

<a id="kafkaquota-entity"></a>
#### `spec.entity`

Identifies the Kafka quota target. At least one field is required.

| Field | Type | Description |
| ----- | ---- | ----------- |
| `user` | string | Specific user in `User:<name>` form (e.g. `User:svc-checkout`). Mutually exclusive with `userDefault` |
| `clientId` | string | Specific client-id. Mutually exclusive with `clientIdDefault` |
| `userDefault` | bool | Target the user-default entity (Kafka null name). Mutually exclusive with `user` |
| `clientIdDefault` | bool | Target the client-id-default entity (Kafka null name). Mutually exclusive with `clientId` |

Allowed entity shapes: user-only, clientId-only, user+clientId, userDefault,
clientIdDefault, and the default/specific combinations. The `User:` prefix on
`user` is stripped when calling Kafka's quota API (consistent with ACL
principals). **Entity immutability:** the entity of a KafkaQuota cannot change
after creation; changing it would orphan the previous entity's quota. Enforced
always-on by the apiserver via a CRD validation rule (whole `entity` block
compared field-wise), and additionally by the operator webhook when enabled
(resolved-entity comparison).

<a id="kafkaquota-limits"></a>
#### `spec.limits`

At least one limit is required. `spec.limits` is **authoritative** for the entity: a
limit key absent from `spec.limits` but present on the live entity is **removed**
(unlike topic config, which is additive — quota dimensions are few and fully owned).
All values must be ≥ 0; `requestPercentage` is not capped at 100.

| Field | Type | Description |
| ----- | ---- | ----------- |
| `producerByteRate` | float64 | Cap produce throughput in bytes/second |
| `consumerByteRate` | float64 | Cap fetch throughput in bytes/second |
| `requestPercentage` | float64 | Cap broker request-handler time as a percent (may exceed 100 on multi-core brokers) |
| `controllerMutationRate` | float64 | Cap create/delete topic+partition mutation rate in mutations/second |

<a id="kafkaquota-reconciliation"></a>
#### Reconciliation

`reconciliation.mode` is honored identically to `KafkaTopic` and
`KafkaAccessPolicy` (see [Reconciliation modes](#reconciliation-modes)).

The diff emits `SetQuota` (entity absent from live), `UpdateQuota` (entity
present but a desired limit value differs or a desired key is missing), and
`RemoveQuota` (live keys not in desired — removed to keep `spec.limits`
authoritative). `SetQuota` and `UpdateQuota` are both implemented via Kafka's
`AlterClientQuotas` API. `RemoveQuota` authoritatively **deletes** a live
limit, so it requires `apply --allow-destructive` (CLI) or the
[`allow-destructive` annotation](#annotations) (operator); without the gate it
is reported as **Blocked** and the live limit stays in place.

**Managed scope** = the entities named in loaded manifests; unmanaged quota
entities are never touched (the partial-loads-never-auto-prune invariant, §9).

`ip` connection-rate quotas are out of scope for this resource.

---

<a id="kafkarolebinding"></a>
## KafkaRoleBinding

Declaratively manages a Confluent RBAC role binding via the **MDS (Metadata
Service) REST API**. MDS is a **Confluent Platform** component — Confluent
Cloud does not expose MDS and authorizes RBAC through a different, cloud-only
API that this tool does not implement, so `KafkaRoleBinding` only works
against self-managed Confluent Platform clusters. RBAC is additive (no Deny)
and spans four scope types: Kafka, Schema Registry, Connect, and ksqlDB.

**CLI + operator (v0.13/v0.14).** `KafkaRoleBinding` is fully supported by
`validate`, `diff`, and `apply` (v0.13) and by the Kubernetes operator (v0.14).
RBAC import (`import cluster`) is planned for v0.16.

**Operator reconciliation (v0.14).** The `KafkaRoleBindingReconciler` controller
reconciles each CR against MDS: compile → diff vs live → apply, with finalizer
cleanup controlled by `spec.deletionPolicy`. The operator manages a
cluster-wide `ClusterRoleBindingView` (the §20.1 analog of `ClusterACLView`) so
one CR's prune scope never removes another CR's bindings; role-binding removal
stays opt-in via `spec.prune: true`.

**Status conditions (operator).** The operator sets the following conditions:

| Condition | Meaning |
| --------- | ------- |
| `Ready` | Aggregate readiness — `True` when the binding is reconciled and MDS is reachable |
| `RoleBindingSynced` | `True` when the desired binding is present in MDS; `False` during convergence or on error |
| `MDSReachable` | `True` when MDS responded successfully; `False` on connectivity/auth failure |
| `ValidationFailed` | `True` when spec fails shape or identity validation (terminal; reconcile pauses until spec changes) |

Phase values: `Pending`, `Ready`, `Error`, `Deleting`.

**Admission webhook (v0.14, `--enable-webhooks`).** The `KafkaRoleBindingValidator`
webhook enforces on create/update:
- **Shape validation** — role, scope type, and resources must be well-formed;
  resource-scoped roles require `resources`; cluster-scoped roles forbid them.
- **Identity uniqueness** — no two `KafkaRoleBinding`s in the cluster may resolve
  to the same `(cluster, principal, role, scope, resourcePattern)`.
- **Immutability** — `clusterRef`, `principal`, `role`, and `scope.type` cannot
  change after creation (changing any would orphan the MDS binding). `resources`
  remains mutable. This identity set is *also* enforced always-on by CRD
  validation rules (`x-kubernetes-validations`) at the apiserver, so it holds
  even with webhooks disabled.

There is no cross-resource conflict check because RBAC is additive (no Deny). If
the cluster is not found or MDS is not configured, the webhook allows the request
and defers to the reconciler. When the webhook is disabled, the reconciler
enforces shape and identity terminally.

**Coexistence with ACLs.** `KafkaRoleBinding` and `KafkaAccessPolicy` are
independent. Both may coexist on the same cluster; the ACL engine and the RBAC
engine are separate. Removing a `KafkaRoleBinding` is prune-gated — see
[RBAC pruning](#rbac-pruning) below.

```yaml
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaRoleBinding
metadata:
  name: checkout-writer
  namespace: team-payments
spec:
  clusterRef:
    name: prod-eu
  principal: User:svc-checkout        # User:<name> | Group:<name>
  role: DeveloperWrite
  scope:
    type: kafka                        # kafka | schema-registry | connect | ksql
  resources:
    - type: Topic
      name: payments.orders
      patternType: literal             # literal (default) | prefixed
  reconciliation:
    mode: Enforce
  prune: false
```

<a id="kafkarolebinding-spec"></a>
### KafkaRoleBinding `spec`

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| [`clusterRef`](#kafkatopic-clusterref) | object | **yes** | The [`KafkaCluster`](#kafkacluster) this binding lives on; the cluster must configure [`authorization.mds`](#kafkacluster-authorization-mds). **Immutable** (always-on CRD validation rule) |
| `principal` | string | **yes** | Principal in `User:<name>` or `Group:<name>` form, e.g. `User:svc-checkout`. **Immutable** (always-on CRD validation rule) |
| `role` | string | **yes** | Confluent RBAC role name (see [Known roles](#kafkarolebinding-roles)); unknown roles are accepted with a warning. **Immutable** (always-on CRD validation rule) |
| [`scope`](#kafkarolebinding-scope) | object | **yes** | MDS scope — selects which sub-cluster the role applies to. `scope.type` is **immutable** (always-on CRD validation rule) |
| [`resources`](#kafkarolebinding-resources) | list | cond. | Resource patterns to bind; required for resource-scoped roles, forbidden for cluster-scoped roles |
| [`reconciliation`](#reconciliation-modes) | object | no | `mode`: `Enforce` (default) \| `DetectOnly` \| `ObserveOnly` |
| `prune` | bool | no | Opt in to role-binding removal when this binding is no longer desired. Default `false` — see [RBAC pruning](#rbac-pruning) |
| `deletionPolicy` | string | no | `Delete` (default) \| `Orphan` |

**Identity.** A `KafkaRoleBinding` is uniquely identified by `(cluster, principal, role, scope, resourcePattern)`. Duplicate identities are a validation error.

<a id="kafkarolebinding-scope"></a>
#### `spec.scope`

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `type` | string | **yes** | `kafka` \| `schema-registry` \| `connect` \| `ksql` |

The scope type selects the sub-cluster ID from `KafkaCluster.spec.authorization.mds.clusters`:
`kafka` uses `kafkaCluster`; `schema-registry` uses `schemaRegistryCluster`; `connect` uses
`connectCluster`; `ksql` uses `ksqlCluster`. The missing sub-cluster ID for a given scope type
is a validation error.

**Valid resource types per scope:**

| `scope.type` | Valid `resources[].type` values |
| ------------ | ------------------------------- |
| `kafka` | `Topic`, `Group`, `Cluster`, `TransactionalId` |
| `schema-registry` | `Subject`, `Cluster` |
| `connect` | `Connector`, `Cluster` |
| `ksql` | `KsqlCluster`, `Cluster` |

<a id="kafkarolebinding-resources"></a>
#### `spec.resources[]`

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `type` | string | **yes** | Resource type (see valid types per scope above) |
| `name` | string | **yes** | Resource name, or prefix when `patternType: prefixed` |
| `patternType` | string | no | `literal` (default) \| `prefixed` |

<a id="kafkarolebinding-roles"></a>
#### Known roles

Confluent canonical roles and their kind:

| Role | Kind | Description |
| ---- | ---- | ----------- |
| `SystemAdmin` | cluster-scoped | Full administrative access to the scope |
| `ClusterAdmin` | cluster-scoped | Cluster-level administrative access |
| `UserAdmin` | cluster-scoped | User and group management |
| `Operator` | cluster-scoped | Operational access (health, metrics, offsets) |
| `SecurityAdmin` | cluster-scoped | Security configuration management |
| `AuditAdmin` | cluster-scoped | Audit log access |
| `ResourceOwner` | resource-scoped | Full control of matched resources |
| `DeveloperRead` | resource-scoped | Read access to matched resources |
| `DeveloperWrite` | resource-scoped | Write access to matched resources |
| `DeveloperManage` | resource-scoped | Manage (create/delete/alter) matched resources |

**Cluster-scoped** roles bind to a whole scope and must not specify `resources`. **Resource-scoped**
roles require at least one entry in `resources`. **Unknown roles** (not in the table above) are
accepted by Monedula with a warning and skip the cluster-vs-resource-scoped check — useful when
Confluent adds a new role before this table is updated.

<a id="rbac-pruning"></a>
#### RBAC pruning (opt-in, prune-gated)

`RemoveRoleBinding` operations (when a binding is dropped from the manifest) are **gated** behind
the same prune opt-in as ACL `DeleteAcl`:

- **CLI:** enable with `apply --prune`. Without it, candidates are only reported
  (`(prune candidate; enable with --prune)`) and apply still succeeds.
- **Operator (v0.14+):** set `spec.prune: true` on the resource.

Without the prune gate, dropping a `KafkaRoleBinding` from your manifests reports it as a
`PruneDisabled` prune candidate but never removes it from MDS — so an accidentally truncated
manifest cannot revoke production access.

---

<a id="kafkauser"></a>
## KafkaUser

Declaratively manages a single Kafka **SCRAM principal**: its username, SASL mechanism, iteration
count, and password. One CR owns one `(cluster, username)` credential 1:1. `KafkaUser` is fully
supported by the CLI (`validate`/`diff`/`apply`, with execute-time password resolution) and by the
Kubernetes operator (v0.35), including an admission webhook and `import cluster` reconstruction.

```yaml
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaUser
metadata:
  name: svc-orders-app
spec:
  clusterRef:
    name: prod-eu
  username: svc-orders-app
  mechanism: SCRAM-SHA-512
  iterations: 8192
  password:
    valueFrom:
      env: ORDERS_APP_PASSWORD    # CLI mode; operator mode uses secretKeyRef instead
  deletionPolicy: Delete
```

<a id="kafkauser-spec"></a>
### KafkaUser `spec`

| Field | Type | Required | Default | Description |
| ----- | ---- | -------- | ------- | ----------- |
| [`clusterRef`](#kafkatopic-clusterref) | object | **yes** | — | The [`KafkaCluster`](#kafkacluster) this principal is created on. **Immutable** (always-on CRD validation rule) |
| [`username`](#kafkauser-username) | string | no | `metadata.name` | The Kafka principal name. **Immutable once set** (always-on CRD validation rule, plus a webhook check for the unset→set nuance below) |
| `mechanism` | string | no | `SCRAM-SHA-512` | `SCRAM-SHA-256` \| `SCRAM-SHA-512` |
| [`iterations`](#kafkauser-iterations) | int | no | *(unset)* | SCRAM iteration count, `4096`–`16384`. Unset means "use the Kafka broker default" and is **never** drift-compared |
| [`password`](#kafkauser-password) | object | **yes** | — | Exactly one of `valueFrom` or `generate` |
| `deletionPolicy` | string | no | `Delete` | `Delete` \| `Orphan` — see [Deletion policies](#deletion-policies) |

**Identity.** A `KafkaUser` is uniquely identified by `(cluster, username)` — mechanism and
iterations are *not* part of the identity, since a Kafka principal owns exactly one credential
set. Two `KafkaUser`s resolving to the same `(cluster, username)` are a collision: a load-time
error in the CLI, and rejected at admission by the webhook (scoped per-namespace unless
`--cluster-namespace` centralizes cluster refs, in which case the scope is cluster-wide).

<a id="kafkauser-username"></a>
#### `spec.username`

Defaults to `metadata.name` when empty. **Immutable once set** — enforced by the Kubernetes
apiserver itself via a CRD validation rule, so it holds even with webhooks disabled: an update from
unset/empty to set is allowed (this is what happens invisibly the moment the manifest is first
applied, since defaulting resolves the empty field to `metadata.name`), but changing or clearing an
already-set value is rejected (a rename is a delete + create of a different Kafka principal,
orphaning the old credential). Because the CEL rule compares against `oldSelf.username`, not
`metadata.name`, it cannot by itself catch an unset→set update to a value *other than*
`metadata.name` — the admission webhook (`--enable-webhooks`) closes that gap by comparing the
**resolved** username (`spec.username`, falling back to `metadata.name`) old vs. new and rejecting
any change. `spec.clusterRef.name` is immutable the same CRD-rule way (repointing a user would
orphan the credential on the previous cluster).

<a id="kafkauser-iterations"></a>
#### `spec.iterations`

Optional, `4096`–`16384`. Left unset, the broker's own default applies and — critically — an unset
`spec.iterations` is **never** treated as drift regardless of what the broker reports back; only a
manifest that explicitly sets a value is compared against the live iteration count.

<a id="kafkauser-password"></a>
#### `spec.password`

Exactly one source, matching the per-mode split used elsewhere in this reference:

| Source | CLI | Operator | Notes |
| ------ | --- | -------- | ----- |
| `valueFrom.env` | ✅ | ❌ | Environment variable |
| `valueFrom.file` | ✅ | ❌ | File path. **Resolves relative to the cluster-config directory** (like [cluster auth](#kafkacluster-auth) credentials) — **not** the manifest's own directory (unlike [schema](#kafkatopic-schema) file refs) |
| `valueFrom.secretKeyRef` | ❌ | ✅ | Kubernetes Secret in the resource's own namespace |
| `valueFrom.inline` | ❌ rejected | ❌ rejected | Always rejected for passwords — an inline password would be committed to Git in plaintext |
| `valueFrom.configMapKeyRef` | ❌ rejected | ❌ rejected | Always rejected for passwords — ConfigMaps are not secret storage |
| `generate` | ❌ rejected | ✅ | Operator-only: the CLI has no way to invent or persist a credential (`spec.password.generate` on a manifest loaded by the CLI is a load-time error) |

```yaml
password:
  valueFrom:
    secretKeyRef:            # operator mode
      name: svc-orders-app-credentials
      key: password
---
password:
  generate: {}               # operator mode only — see "Generated Secret" below
```

**Rotation is event-driven, never drift-detected.** Kafka's `DescribeUserSCRAMCredentials` API
never exposes the password itself — only the username, mechanism, and iteration count are
observable — so the password is entirely outside the drift surface. There is nothing to compare
a manifest against, and a `KafkaUser` in sync on username/mechanism/iterations is reported as
fully in-sync even if the underlying password value has changed. Instead, rotation happens on an
explicit trigger:

- **Operator (`valueFrom.secretKeyRef`)**: the referenced Secret's `resourceVersion` is tracked in
  `status.appliedPasswordRef`; a change to that resourceVersion re-upserts the SCRAM credential
  with the new value at the next reconcile (promptly, if the Secret carries the
  [`credential-source` label](#kafkacluster-tenancy) watch — see [operator docs](operator.md)).
- **CLI**: pass `apply --rotate-passwords` to re-resolve and re-upsert the password of every
  declared, in-sync `KafkaUser` from its configured source. Users with identity drift are updated
  regardless of the flag. Password values are resolved at **execute time**, immediately before the
  SCRAM upsert call — never at diff/plan time, and never included in rendered plan output.

**Generated Secret contract (operator, `generate` mode).** The operator creates and owns a
Kubernetes Secret named **`<KafkaUser name>-kafka-credentials`**, holding keys `username`,
`password`, and `mechanism`. The Secret is created **before** the SCRAM upsert call on every
reconcile that needs one, so a crash between the two leaves a recoverable "stored but not yet
applied" password rather than a broker-only credential nobody can read back. It carries an owner
reference to the `KafkaUser` (garbage-collected automatically on CR deletion — the operator never
explicitly deletes it) and the `gitops.monedula.dev/credential-source: "true"` label. **A password
is regenerated only when that Secret is deleted** — deleting it is the explicit, user-initiated
rotation request; editing the CR spec itself never regenerates the password. A pre-existing Secret
at that name that is **not** owned by this `KafkaUser` is never adopted or overwritten: the
reconciler reports a terminal error naming the foreign Secret instead.

<a id="kafkauser-mechanism-swap"></a>
#### Mechanism changes and the partial-mechanism-change residue

Changing `spec.mechanism` on an existing `KafkaUser` (e.g. `SCRAM-SHA-256` → `SCRAM-SHA-512`) is a
single `Medium`-risk, ungated operation: the new mechanism is upserted first, then the old
mechanism's credential is deleted. If the delete step fails after the upsert succeeds (e.g. a
transient broker error), the live principal is left with **both** mechanisms configured — the new
one, which is desired, and the old one, which is now residue. This residue is invisible to the
next `diff`/`verify`: `KafkaUser` diff scope is **declared-mechanism-only** (the mechanism named in
the manifest), so an extra live mechanism the manifest does not name is never reported as drift and
never pruned — the same way an undeclared live user is never reported. The stale mechanism must be
removed manually (or via the operator's finalizer path on deletion); re-running `apply` will not
retry or surface it.

<a id="kafkauser-examples"></a>
#### Example manifests

Referenced password (CLI, env; operator, `secretKeyRef`):

```yaml
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaUser
metadata:
  name: svc-orders-app
spec:
  clusterRef:
    name: prod-eu
  username: svc-orders-app
  mechanism: SCRAM-SHA-512
  password:
    valueFrom:
      env: ORDERS_APP_PASSWORD   # CLI: env|file; operator: secretKeyRef
```

Operator-generated password:

```yaml
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaUser
metadata:
  name: svc-orders-app
spec:
  clusterRef:
    name: prod-eu
  username: svc-orders-app
  mechanism: SCRAM-SHA-512
  password:
    generate: {}    # operator creates + owns svc-orders-app-kafka-credentials
```

---

<a id="valuefrom"></a>
## `valueFrom` (secret references)

Credential and schema-body fields take a `valueFrom` with exactly one source:

```yaml
valueFrom:
  env: KAFKA_PASSWORD          # CLI mode: environment variable
---
valueFrom:
  file: ./relative/or/abs.txt  # CLI mode: file (relative to the manifest / config dir)
---
valueFrom:
  secretKeyRef:                # Operator mode: Kubernetes Secret in the
    name: kafka-credentials    # resource's own namespace
    key: kafka-password
---
valueFrom:
  inline: |                    # The literal value itself, taken verbatim (e.g.
    {"type":"record","name":"Order","fields":[]}   # schema bodies). Rejected for credential fields.
---
valueFrom:
  configMapKeyRef:             # Operator mode: ConfigMap (schema bodies)
    name: orders-schemas
    key: orders-value.avsc
```

| Source | CLI | Operator | Notes |
| ------ | --- | -------- | ----- |
| `env` | ✅ | ❌ rejected | Environment variable |
| `file` | ✅ (confined against traversal) | ❌ rejected | File path |
| `secretKeyRef` | ❌ rejected | ✅ | Kubernetes Secret |
| `inline` | ✅ | ✅ | **The literal-value form** — the value is taken verbatim from the manifest, no indirection. This is the "plain value" field for `valueFrom`-typed sources: use it for non-secret strings (schema bodies, and any other non-credential value a `valueFrom` field accepts). **Credential fields (e.g. `KafkaUser.spec.password`) reject `inline`** — see [`spec.password`](#kafkauser-password) — since an inline credential would be committed to Git in plaintext |
| `configMapKeyRef` | ❌ rejected | ✅ (schema bodies) | Kubernetes ConfigMap |

**Prompt reconcile via the ConfigMap watch (operator mode).** A ConfigMap that
holds a schema body should carry the label
`gitops.monedula.dev/schema-source: "true"`. The operator maintains a
label-scoped watch: any change to a labelled ConfigMap immediately enqueues the
`KafkaTopic`(s) in the same namespace that reference it via
`spec.schema.valueSchema.valueFrom.configMapKeyRef` (or `keySchema`), so the
schema body is re-registered without waiting for the periodic resync. Without
the label, body edits are applied at the next resync (≤ 5 min), and the topic
reports the `SchemaSourceUnwatched` condition (`True`, reason
`ConfigMapNotLabeled`) as an informational signal — reconciliation is never
blocked, only delayed. See `config/samples/gitops_v1alpha1_kafkatopic_schema_configmap.yaml`
for a worked example.

**Prompt reconcile via the Secret watch (operator mode).** A credential/TLS
Secret referenced by a `KafkaCluster` (SASL/OAuth auth, mTLS certs, Schema
Registry auth, MDS auth) is read at reconcile time, but a rotation is otherwise
picked up only at the periodic resync. Label the Secret with
`gitops.monedula.dev/credential-source: "true"` to opt it into the operator's
prompt watch: the operator caches only labelled Secrets and, on a change,
re-reconciles the referencing `KafkaCluster` AND every data-plane resource on it
(2-hop fan-out). Reads are uncached, so an unlabelled Secret still resolves (it
just converges at the resync); a `CredentialSourceUnwatched` condition on the
`KafkaCluster` names any referenced Secret missing the label.

<a id="secretkeyref"></a>
`secretKeyRef` (`{name, key}`) is the Secret-backed `valueFrom` source used by
credential and TLS fields (e.g. [`tls.caCert`](#kafkacluster-tls)) in operator
mode. Resolved values are never logged.

---

<a id="reconciliation-modes"></a>
## Reconciliation modes

`spec.reconciliation.mode` on [`KafkaTopic`](#kafkatopic-spec),
[`KafkaAccessPolicy`](#kafkaaccesspolicy-spec), [`KafkaQuota`](#kafkaquota-spec),
and [`KafkaRoleBinding`](#kafkarolebinding-spec);
honored identically by the CLI and the operator.

| Mode | Behavior |
| ---- | -------- |
| `Enforce` (default) | Reconcile Kafka to match the manifest |
| `DetectOnly` | Report drift (CLI: exit 1 on `verify`; operator: phase `Drifted`) but never mutate |
| `ObserveOnly` | Observe live state; drift is reported but is not a failure |

<a id="deletion-policies"></a>
## Deletion policies

`spec.deletionPolicy` controls what happens to the **external Kafka state**
when the resource is removed (operator: on CR deletion via the finalizer).

| Kind | Default | `Orphan` | `Delete` |
| ---- | ------- | -------- | -------- |
| [`KafkaTopic`](#kafkatopic) | `Orphan` | Leave the topic + its ACLs in Kafka | Delete topic-local ACLs, then the topic — requires the [`allow-delete`](#annotations) annotation |
| [`KafkaAccessPolicy`](#kafkaaccesspolicy) | `Delete` | Leave the ACLs | Delete the policy's managed ACLs — requires [`allow-delete`](#annotations) |
| [`KafkaRoleBinding`](#kafkarolebinding) | `Delete` | Leave the MDS role binding(s) in place | Remove the MDS role binding(s) — **ungated** (no `allow-delete` needed): deleting the CR is itself the explicit action, since the MDS bindings are this resource's entire reason to exist |
| [`KafkaUser`](#kafkauser) | `Delete` | Leave the SCRAM credential in place | Delete the declared mechanism's SCRAM credential — **ungated** (no `allow-delete` needed): deleting the CR is itself the explicit action, since the credential is this resource's entire reason to exist |

<a id="pruning"></a>
## ACL pruning (opt-in)

Pruning deletes in-scope ACLs that are no longer desired (e.g. a principal
removed from `access` or `rules`). It is **disabled by default**, so an
accidentally truncated manifest cannot cut production access:

- **CLI:** enable with `apply --prune`. Without it, candidates are only
  reported (`(prune candidate; enable with --prune)`) and apply still succeeds.
- **Operator:** set `spec.prune: true` on the resource. The operator computes
  prune candidates against the cluster-wide desired-ACL union across all
  resources, and a candidate is deleted only when **every** resource whose
  scope covers it has opted in; otherwise it is reported as `PruneDisabled`
  drift.

Pruning never touches ACLs outside the managed scope, and it is **not** gated
by [`allow-destructive`](#annotations) — the prune opt-in is the gate.

<a id="managed-scope-edge"></a>
### Managed-scope edge: hand-made ACLs on a manifest-referenced pair

Managed scope is keyed on **(principal, resource pattern)**, not on which tool
created the ACL. If a manifest declares access for `User:svc-checkout` on
`Topic:orders`, then *every* live ACL for that exact `(User:svc-checkout,
Topic:orders)` pair is in scope — including ACLs you created by hand outside
GitOps, before the manifest ever existed.

```yaml
# manifest declares:
access:
  producers:
    - principal: User:svc-checkout   # -> ACL on Topic:orders

# live cluster additionally has a hand-made ACL, e.g. granted via kafka-acls.sh:
#   Allow User:svc-checkout to Read on Topic:orders from host 10.0.0.0/8
```

Once the manifest above is applied, `svc-checkout` + `orders` is GitOps-owned
as a *pair*, not just for the specific ACL the manifest wrote. The hand-made
`Read` grant is not in the manifest, so it becomes a prune candidate the next
time `--prune`/`spec.prune` runs — even though GitOps never created it.

**Mitigation:**
- Bring hand-made grants under management explicitly (`import cluster`, or
  hand-author the equivalent `access`/`KafkaAccessPolicy` entry) so they show
  up as desired state instead of drift.
- Or use a distinct principal for anything that must stay outside GitOps'
  reach — scope ownership is per-pair, so a different principal (or, for a
  prefixed pattern, a non-overlapping prefix) keeps the hand-made grant
  entirely out of the manifest's managed scope.

<a id="multi-pipeline-ownership"></a>
### Multi-pipeline ownership: one owner per (principal, pattern)

The same per-pair scoping means a `(principal, resource pattern)` must be
owned by exactly **one** applied manifest set. If two teams run separate
pipelines that both grant the same shared principal against the same
resource, each pipeline's prune pass sees the other's grant as an
undeclared-therefore-prunable ACL:

```text
team-a/manifests/  ->  applied with --prune, grants User:shared-svc on Topic:orders
team-b/manifests/  ->  applied with --prune, grants User:shared-svc on Topic:orders
```

Whichever pipeline runs last wins the tuple; the other's next `--prune` run
removes the grant it does not recognize, and the two pipelines flap the ACL
back and forth. This is not a bug in either pipeline — both are correctly
pruning what they don't declare.

**Mitigation:** keep every `(principal, pattern)` pair declared in exactly one
repository/pipeline. If a principal is genuinely shared across teams, put its
grants in one shared manifest set that both teams contribute to via normal
code review, rather than letting two independently-applied pipelines both
claim ownership of it.

<a id="annotations"></a>
## Annotations

Set on the individual resource (`metadata.annotations`); the CLI equivalents
are the `--allow-delete` / `--allow-destructive` flags on `apply`.

| Annotation | Effect |
| ---------- | ------ |
| `gitops.monedula.dev/allow-destructive: "true"` | Permit gated destructive operations: partition increase, replication-factor change, schema compatibility lowering (including a first-time set below the registry's global default), subject deletion, quota limit removal. (ACL pruning is **not** covered — it has its own [opt-in](#pruning)) |
| `gitops.monedula.dev/allow-delete: "true"` | Permit topic / subject / policy-ACL / MDS role binding deletion (with `deletionPolicy: Delete`) |
| `gitops.monedula.dev/force-finalizer-removal: "true"` | Operator only: remove the finalizer even when the cluster is unreachable (skips external cleanup) |

Without the required gate, the operation is reported as **Blocked** (CLI:
`apply` exits 3 when blocked operations are the only problem; operator:
`Ready=False` in status) and nothing is mutated.
