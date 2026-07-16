# Connecting to Kafka, Schema Registry, and MDS

How a `KafkaCluster` describes connectivity and authorization — broker TLS/SASL,
RBAC role bindings, and the topic-access → RBAC auto-map.

## Support matrix

| Capability | Apache Kafka | Confluent Platform | Confluent Cloud |
|---|:---:|:---:|:---:|
| Topics / ACLs | ✅ | ✅ | ✅ validated 2026-07-09 |
| Quotas / SCRAM users | ✅ | ✅ | ❌ (not exposed by Cloud) |
| Schema Registry | ✅ | ✅ | ✅ validated 2026-07-09 |
| MDS/RBAC role bindings | ❌ (no MDS) | ✅ | ❌ (different API, not implemented) |

**Topics / ACLs / quotas / SCRAM users.** These features use the standard Kafka Admin API
(`CreateTopics`, `CreateAcls`, `AlterClientQuotas`, `AlterUserScramCredentials`, etc. — see
[Least-privilege connecting principal](#least-privilege-connecting-principal) for the exact
calls). Confluent Cloud exposes much of this surface through the same Admin API protocol, but it
restricts or omits parts of it — notably SCRAM credential management is not available, and Cloud
rejects client-quota describes (`DescribeClientQuotas`) outright with
`CLUSTER_AUTHORIZATION_FAILED`. Topic-only pipelines still work there because live reads are
scoped to the kinds the manifests declare (a manifest set without `KafkaQuota` never issues the
quota describe), and `import cluster --skip-quotas` skips the call on import.

**ACL principals on Confluent Cloud must use the service account's NUMERIC id**
(`User:9207877`), never the `sa-xxxxxx` resource id. Cloud stores Kafka-protocol ACLs under the
numeric id: `CreateAcls` with a `User:sa-...` principal is **acknowledged and then silently
dropped** — the apply reports success but no ACL is created — and `DescribeAcls` returns the
numeric form, so an `sa-` principal in a manifest would show as permanently missing even if it
had persisted. The Cloud CLI and console translate between the two forms; the raw Kafka protocol
does not (validated against a live Basic cluster, 2026-07-09).

Topics and ACLs were validated against a live Confluent Cloud Basic cluster on 2026-07-09 via the
maintainer harness: topic create/alter, restricted-config rejection, out-of-band drift detection
and reconciliation, ACL grant/no-diff/prune (with a numeric-id principal), and import round-trip
all pass; quota and SCRAM operations fail cleanly as described above.

**Schema Registry.** Registration and compatibility governance only require the target registry to
speak the Confluent Schema Registry REST API. That includes self-hosted Confluent Schema Registry,
Confluent Platform's bundled registry, and (in principle) Confluent Cloud's managed Schema
Registry, since the client only depends on API compatibility. Validated against Confluent Cloud's
managed Schema Registry on 2026-07-09 (registration + compatible evolution to a second version).

**MDS/RBAC role bindings.** `KafkaRoleBinding` and the `accessBackends: [rbac]` auto-map talk to
Confluent's **MDS (Metadata Service) REST API**, which is a Confluent Platform-only component —
there is no MDS on Apache Kafka, and Confluent Cloud does not run MDS at all. Cloud has its own
RBAC system with a different, cloud-specific API that this tool does not implement. If you are
evaluating RBAC support against Confluent Cloud, it is out of scope today.

**What CI actually validates.** The scenario suite runs continuously against Apache Kafka
(`cp-kafka` images) and Confluent Platform components (Schema Registry, MDS). Confluent Cloud is
not part of CI — its cells in the matrix come from manual runs of the opt-in, credential-gated
validation harness at [`test/e2e/cloud/`](../test/e2e/cloud/README.md) (`make e2e-cloud`), most
recently 2026-07-09 against a Basic cluster; the matrix is only updated after a real run.

## Connecting to Kafka

A `KafkaCluster` supplied via `--cluster-config-file` describes how to reach the
broker. Bootstrap servers are required; TLS and SASL are optional.

```yaml
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaCluster
metadata:
  name: prod
spec:
  bootstrapServers: broker1:9093,broker2:9093
  tls:
    enabled: true
    # Private CA? Point caCert at the CA bundle (omit to use the system trust store):
    caCert:
      valueFrom:
        file: certs/ca.crt
  auth:
    mechanism: SCRAM-SHA-512   # or SCRAM-SHA-256, PLAIN, None
    scram:
      username:
        valueFrom:
          env: KAFKA_USER
      password:
        valueFrom:
          file: /etc/secrets/kafka-password
```

- **TLS:** set `spec.tls.enabled: true` to use TLS with the system trust store.
  To trust a private CA, set `tls.caCert` to a `valueFrom` reference to the CA
  PEM (bundle): `file`/`env`/`inline` in CLI mode, `secretKeyRef` in operator
  mode. `insecureSkipVerify: true` disables server verification entirely and is
  a dev-only escape hatch — prefer `caCert`.
- **SASL:** `spec.auth.mechanism` may be `PLAIN`, `SCRAM-SHA-256`,
  `SCRAM-SHA-512`, or `OAUTHBEARER` (`None`/empty disables SASL). SCRAM/`PLAIN`
  credentials come from the `spec.auth.scram.{username,password}` block;
  `OAUTHBEARER` uses the OIDC client-credentials flow via
  `spec.auth.oauth.{tokenEndpoint, clientId, clientSecret, scope}`. If the
  identity provider's `tokenEndpoint` serves TLS from a private CA, set
  `spec.auth.oauth.tokenEndpointCA` to a `valueFrom` reference to that CA's PEM
  — the IdP is a separate trust domain from the Kafka brokers, so this is
  never derived from `spec.tls.caCert`; omit it to trust the system root
  store (the default).
- **mTLS:** set `spec.auth.mechanism: mTLS` with `spec.tls.enabled: true` and a
  client certificate/key under `spec.tls.clientCert` / `spec.tls.clientKey`
  (both `valueFrom` references, set together). The client authenticates with the
  certificate; no SASL is used.
- **Secret references** resolve from environment variables (`valueFrom.env`) or
  files (`valueFrom.file`, relative to the cluster-config directory) in CLI mode.
  Kubernetes `secretKeyRef` is resolved only by the
  [operator](operator.md#credentials-via-kubernetes-secrets), not in CLI mode.
  Resolved secret values are never logged.

## Connecting to Schema Registry

The optional `spec.schemaRegistry` block configures the Schema Registry
connection used for `KafkaTopic.spec.schema` subjects. It supports HTTP Basic
auth and TLS with the same shape as the broker and MDS connections:

```yaml
spec:
  schemaRegistry:
    endpoint: "https://sr.example.com:8081"
    auth:
      type: basic
      username: { valueFrom: { env: SR_USER } }
      password: { valueFrom: { env: SR_PASSWORD } }
    tls:
      enabled: true
      # Private CA? Point caCert at the CA bundle (omit to use the system trust store):
      caCert:
        valueFrom:
          file: certs/sr-ca.crt
      # Optional TLS client certificate (set clientCert and clientKey together):
      # clientCert: { valueFrom: { file: certs/sr-client.crt } }
      # clientKey:  { valueFrom: { file: certs/sr-client.key } }
```

`schemaRegistry.tls` works exactly like `spec.tls`: `enabled: true` uses the
system trust store, `caCert` trusts a private CA (resolved `file`/`env`/`inline`
in CLI mode, `secretKeyRef` in operator mode), and `insecureSkipVerify: true`
is a dev-only escape hatch — prefer `caCert`. The same applies to
`authorization.mds.tls` for the MDS connection.

## RBAC role bindings (v0.13 CLI / v0.14 operator)

To manage Confluent RBAC role bindings, add an `authorization.mds` block to the
`KafkaCluster` config and create `KafkaRoleBinding` manifests:

```yaml
# clusters/prod.yaml
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaCluster
metadata:
  name: prod
spec:
  bootstrapServers: broker1:9093
  tls:
    enabled: true
  auth:
    mechanism: SCRAM-SHA-512
    scram:
      username: { valueFrom: { env: KAFKA_USER } }
      password: { valueFrom: { env: KAFKA_PASSWORD } }
  authorization:
    mds:
      endpoint: "https://mds.example.com:8090"
      auth:
        type: basic
        username: { valueFrom: { env: MDS_USER } }
        password: { valueFrom: { env: MDS_PASSWORD } }
      clusters:
        kafkaCluster: "abc123"
```

```yaml
# rbac/checkout-writer.yaml
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaRoleBinding
metadata:
  name: checkout-writer
spec:
  clusterRef:
    name: prod
  principal: User:svc-checkout
  role: DeveloperWrite
  scope:
    type: kafka
  resources:
    - type: Topic
      name: payments.orders
      patternType: literal
```

```sh
# validate
monedula-gitops validate -f ./rbac --cluster-config-file ./clusters/prod.yaml

# preview
monedula-gitops diff -f ./rbac --cluster-config-file ./clusters/prod.yaml

# apply (role binding removal also requires --prune)
monedula-gitops apply -f ./rbac --cluster-config-file ./clusters/prod.yaml
```

RBAC coexists with ACLs — ACL manifests and RBAC manifests can be loaded
together in the same `-f` invocation; the engines are independent.
Role binding **removal** is prune-gated: without `--prune`, candidates are
reported as `PruneDisabled` drift but never deleted, protecting against an
accidentally truncated manifest revoking production access.

The `KafkaRoleBinding` operator controller + admission webhook shipped in v0.14 —
see the [Kubernetes operator](operator.md) page for details.

### Importing RBAC (v0.16)

`import cluster` reconstructs live MDS role bindings alongside topics. Unambiguous producer and
consumer patterns fold back into `KafkaTopic.spec.access` (the symmetric grants that `accessBackends`
would auto-map in the forward direction): a `DeveloperWrite` on a topic becomes an `access.producers`
entry; a `DeveloperRead` on a topic paired 1:1 with a `DeveloperRead` on a group becomes an
`access.consumers` entry. Everything that cannot fold cleanly — cluster-scoped roles, `ResourceOwner`,
SR/Connect/ksqlDB scopes, ambiguous consumer pairings, custom/unknown roles, prefixed patterns —
is emitted as an explicit `KafkaRoleBinding`.

A two-sided recompile-verify confirms that compiling the generated manifests reproduces the live
role-binding set exactly (RBAC side) and the live ACL set exactly (ACL side, on dual clusters).
On any mismatch the importer falls back to all-explicit representation, guaranteeing that a
subsequent `apply` never adds or drops a grant on either backend.

No `--skip-rbac` flag is needed: absent `authorization.mds` on the cluster config is the off switch.

### accessBackends — topic-access → RBAC auto-map (v0.15)

`KafkaCluster.spec.authorization.accessBackends` controls how a
`KafkaTopic.spec.access` block is realized:

| `accessBackends` value | ACLs emitted | MDS role bindings emitted |
| ---------------------- | :----------: | :-----------------------: |
| unset / `[acl]` (default) | yes | no |
| `[rbac]` | no | yes |
| `[acl, rbac]` | yes | yes |

`rbac` requires `authorization.mds` to be configured. The auto-map rules are:

- **producer** entry → one MDS role binding: `DeveloperWrite` on `{Topic, <topicName>, literal}`.
- **consumer** entry → two MDS role bindings: `DeveloperRead` on `{Topic, <topicName>, literal}`
  **and** `DeveloperRead` on `{Group, <group>, literal}`.

These bindings are emitted alongside any `KafkaRoleBinding` CRs for the same cluster; RBAC is
additive so there is no conflict. Derived bindings participate in the cluster-wide role-binding
prune scope (`ClusterRoleBindingView`) just like explicit bindings.

**Coarsening caveat.** RBAC has no host concept and roles are coarse bundles. If an access entry
specifies a non-`*` host or custom operation lists (`operations` / `topicOperations` /
`groupOperations`), the binding is still emitted (host/operation refinement dropped) but a
`RBACCoarsened=True` condition is set on the `KafkaTopic` status and a warning is printed by the
CLI. Use a `KafkaAccessPolicy` for host-scoped grants on rbac-backed clusters.

**Dual-backend revocation.** On a cluster with `accessBackends: [acl, rbac]`, Kafka's authorizer
grants access if **either** backend allows it — ACLs and MDS role bindings are independent, additive
authorization sources, not two views of one decision. That has a direct consequence for revocation:
removing an `access` entry (or a `KafkaRoleBinding`) only closes the backend it targets. If a
principal's ACL is pruned but its equivalent MDS role binding is left in place (or vice versa), the
principal **keeps access** through the surviving backend — silently, since neither `diff` nor
`verify` treats "still authorized via the other backend" as an error; each backend's drift is
computed independently.

```yaml
# Revoking svc-checkout's producer access on a [acl, rbac] cluster:
# BEFORE — access block present, dual-emits an ACL + a DeveloperWrite role binding
access:
  producers:
    - principal: User:svc-checkout

# AFTER — entry removed entirely
access:
  producers: []
```

```sh
# Both backends must be pruned in the same apply for the revocation to take effect.
# Omitting --prune (or applying against a cluster config where rbac was already
# turned off) leaves the other backend's grant live.
monedula-gitops apply -f ./topic.yaml --cluster-config-file ./cluster.yaml --prune
```

Because of this, treat `accessBackends: [acl, rbac]` as a **migration posture** — a deliberate,
temporary dual-emit while moving a cluster from ACLs to RBAC (or the reverse) — rather than a
steady state to run indefinitely. While dual-emit is active, any access change (grant *or* revoke)
must be verified against both backends: `diff`/`verify` report ACL and role-binding operations
separately, so check that a removal shows up as both a `RemoveAcl`-family operation *and* an MDS
role-binding removal before assuming the principal has lost access.

**Worked example.** A cluster with `accessBackends: [acl, rbac]` and a topic with producer +
consumer entries:

```yaml
# Example KafkaCluster with dual ACL + RBAC backends (spec §40, shipped v0.15).
# authorization.accessBackends: [acl, rbac] makes KafkaTopic.spec.access dual-emit:
#   - ACLs via the Kafka Admin API (the existing path)
#   - MDS role bindings via authorization.mds (producer→DeveloperWrite,
#     consumer→DeveloperRead on topic + group)
#
# accessBackends defaults to [acl] when unset, preserving back-compat.
# [rbac] emits role bindings only; [acl, rbac] emits both (shown here).
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaCluster
metadata:
  name: prod
spec:
  bootstrapServers: kafka-bootstrap.kafka.svc:9093
  tls:
    enabled: true
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
  authorization:
    mds:
      endpoint: https://kafka-mds.kafka.svc:8090
      auth:
        type: basic
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
      clusters:
        kafkaCluster: abc123
    accessBackends:
      - acl
      - rbac
```

```yaml
# Example KafkaTopic that leverages the accessBackends dual-emit (spec §40, v0.15).
# When the cluster has accessBackends: [acl, rbac], this access block auto-maps to:
#
#   Producer User:svc-checkout:
#     - ACL: Allow Write on Topic:orders (host *)
#     - MDS role binding: DeveloperWrite on {Topic, orders, literal} in kafka scope
#
#   Consumer User:svc-fulfillment (group: fulfillment):
#     - ACL: Allow Read on Topic:orders + Group:fulfillment (host *)
#     - MDS role bindings:
#         DeveloperRead on {Topic, orders, literal}
#         DeveloperRead on {Group, fulfillment, literal}
#
# RBAC-unrepresentable access (non-* host, custom operation lists) is warn-and-mapped:
# the binding is emitted (host/op refinement dropped) and RBACCoarsened=True is set
# on the topic status (§21).
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaTopic
metadata:
  name: orders
  namespace: team-a
spec:
  clusterRef:
    name: prod
  topicName: orders
  partitions: 6
  config:
    retention.ms: "604800000"
  access:
    producers:
      - principal: User:svc-checkout
    consumers:
      - principal: User:svc-fulfillment
        group: fulfillment
```

```sh
# validate
monedula-gitops validate -f ./topic.yaml \
  --cluster-config-file ./cluster.yaml

# preview dual-emit (CreateAcl + AddRoleBinding)
monedula-gitops diff -f ./topic.yaml \
  --cluster-config-file ./cluster.yaml
```

## Least-privilege connecting principal

The fastest way to get started is a Kafka **super-user** (`super.users`) — it bypasses ACL
enforcement entirely, so `diff`/`apply` never trips over its own missing permissions. That is
fine for evaluation and single-team clusters, but most security teams will want the tool's
principal scoped down to exactly what it does. The tables below list the Kafka ACL operations
each feature area needs, derived directly from the Admin API calls the tool issues
(`internal/kafka/franz/client.go`).

The permission surface is **scoped to the kinds your manifests declare**: `diff`/`verify`/`apply`
only read live state for a kind when at least one manifest of that kind is loaded. A topics-only
pipeline issues no ACL, quota, SCRAM, Schema Registry, or MDS describe calls, so its principal
needs only the topic rows below — it does not need `Describe`/`DescribeConfigs` on `Cluster` for
ACLs or quotas it never manages. (Real-world case: Confluent Cloud rejects `DescribeClientQuotas`
outright, and an unconditional quota read used to fail even topic-only applies there.)

**Topics** (`KafkaTopic`):

| Action | Required ACL | Resource |
| --- | --- | --- |
| Read live state (`diff`/`verify`, `ListTopics`/`GetTopic`) | `Describe` + `DescribeConfigs` | the topic (or `Cluster` — see caveat below) |
| Create a topic (`CreateTopic`) | `Create` | the topic name, or `Create` on `Cluster` for unrestricted create |
| Change topic config (`UpdateTopicConfig`) | `AlterConfigs` | the topic |
| Increase partitions (`CreatePartitions`) | `Alter` | the topic |
| Delete a topic (`DeleteTopic`, `deletionPolicy: Delete`) | `Delete` | the topic |

**ACL management** (`KafkaAccessPolicy`, `KafkaTopic.spec.access`):

| Action | Required ACL | Resource |
| --- | --- | --- |
| List/read ACLs (`ListACLs`) | `Describe` | `Cluster` |
| Create/delete ACLs (`CreateACLs`/`DeleteACLs`) | `Alter` | `Cluster` |

Kafka's ACL API has no per-resource-type authorization of its own — reading or writing *any*
ACL, on any resource, requires `Describe`/`Alter` on the `Cluster` resource. There is no way to
grant "manage ACLs for topics matching `payments.*`" without granting cluster-wide ACL
management. If your GitOps principal manages ACLs at all, it is powerful in this one specific
sense — scope what topics/consumer-groups it can touch via the manifests and namespace/tenancy
controls (`allowedNamespaces`, §20.2), not via Kafka ACLs on the ACL API itself.

**Quotas** (`KafkaQuota`):

| Action | Required ACL | Resource |
| --- | --- | --- |
| List quotas (`ListQuotas` → `DescribeClientQuotas`) | `DescribeConfigs` | `Cluster` |
| Set/remove quotas (`SetQuota`/`DeleteQuota` → `AlterClientQuotas`) | `AlterConfigs` | `Cluster` |

Like ACLs, the client-quota API is cluster-scoped — there is no per-entity (user/client-id/IP)
ACL granularity.

**SCRAM credential management** (`KafkaUser`):

| Action | Required ACL | Resource |
| --- | --- | --- |
| List credentials (`ListScramCredentials` → `DescribeUserSCRAMs`) | `Describe` | `Cluster` |
| Create/update/delete a credential (`UpsertScramCredential`/`DeleteScramCredential` → `AlterUserSCRAMs`) | `Alter` | `Cluster` |

Per [KIP-554](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554:+Add+Broker-side+SCRAM+Config+API),
which added these broker-side RPCs, `DescribeUserScramCredentials` requires `Describe` on
`Cluster` and `AlterUserScramCredentials` requires `Alter` on `Cluster` — **not**
`DescribeConfigs`/`AlterConfigs` (those govern topic/broker/client-quota *configs*; SCRAM
credentials are a separate authorization surface). Like ACLs and quotas, this is cluster-scoped
with no per-username granularity — a principal that can manage any `KafkaUser` can create or
rotate the credential for any Kafka username.

**Schema Registry** (`KafkaTopic.spec.schema`):

Schema Registry authorization is HTTP-level, not a Kafka ACL. `spec.schemaRegistry.auth`
(Basic auth) and `spec.schemaRegistry.tls` (client certificates) authenticate the tool to SR;
what that identity is allowed to do is enforced by SR's own authorizer, if one is configured
(e.g. Confluent's SR RBAC or a reverse-proxy ACL layer). The tool needs subject read (`GET`),
register (`POST`), and compatibility-config read/write on the subjects it manages — consult your
Schema Registry's authorization documentation for the exact role/permission names.

**MDS/RBAC** (`KafkaRoleBinding`, `accessBackends: [rbac]`):

The tool lists, adds, and removes Confluent role bindings via MDS. That requires the MDS
principal to hold a Confluent RBAC role with role-binding management rights at the relevant
scope (cluster or resource) — typically `UserAdmin` or `SecurityAdmin`, per your Confluent
Platform version. This guide intentionally doesn't enumerate exact role/API details here, since
they vary by Confluent Platform version and deployment — see the
[Confluent RBAC documentation](https://docs.confluent.io/platform/current/security/rbac/index.html)
for the authoritative mapping of roles to what they permit.

**Caveats:**

- Kafka's authorizer treats `Create`/`Describe`/`DescribeConfigs`/`Alter`/`AlterConfigs` granted
  on the `Cluster` resource as applying broadly (e.g. `Create` on `Cluster` permits creating any
  topic, not just specific ones) — granting at `Cluster` scope is simpler to set up but wider
  than granting per-topic/per-prefix ACLs. Choose based on how much topic-name structure your
  organization already enforces.
- `Delete` on a topic is required only if you use `deletionPolicy: Delete`; the default
  `Orphan` policy never issues a Kafka delete, so a principal without `Delete` still works for the
  common case.
- ACL and quota management fundamentally require `Alter`/`AlterConfigs` on `Cluster` — there is
  no narrower Kafka-native grant. If that is unacceptable for your threat model, keep ACL/quota
  management on a super-user or a dedicated high-trust principal, and run topic-only management
  (no `KafkaAccessPolicy`/`KafkaQuota` manifests) with a narrowly-scoped principal instead.
- **Self-lockout hazard:** a non-super-user principal that creates a topic's ACLs without
  including itself can lose read/write access to that topic the moment the ACLs are applied
  (`allow.everyone.if.no.acl.found=true` semantics — a resource is open only while it has no
  ACLs). The CLI and operator print an advisory warning when the desired ACL set omits the
  connecting principal, but the check is client-side and cannot see broker-side `super.users`
  configuration.
