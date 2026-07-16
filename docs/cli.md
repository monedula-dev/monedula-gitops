# CLI reference

The `monedula-gitops` command-line tool validates, diffs, and applies Kafka
GitOps manifests against a live cluster.

## Commands

All commands accept `-f/--filename` (repeatable; a file or directory),
`-R/--recursive` to recurse into directories, and `-o/--output` to select the
output format (`human`, `yaml`, or `json`). Commands that compare against live
state also accept `-c`/`--cluster-config-file` (repeatable) and `--cluster` to
select a single cluster by name. The global `--log-level error|warn|info|debug` flag
(default `warn`) controls structured diagnostic logs on stderr — role-name
typos and RBAC-coarsening warnings are visible by default; `debug` also
enables the Kafka client's own logs. Stdout output documents stay clean at any
level.

The examples below run against the fixtures under `testdata/`.

### validate

Parse and validate manifests (and cluster references when a cluster config is
supplied).

```sh
monedula-gitops validate -f ./testdata/valid
```

### diff

Show the operations needed to bring live state in line with the manifests.
`diff` always exits 0 regardless of drift.

```sh
monedula-gitops diff -f ./testdata/valid --cluster-config-file ./testdata/clusters/dev.yaml
```

`diff`, `verify`, and `apply` read live state **only for the kinds the loaded
manifests declare**: a topics-only manifest set issues no ACL, quota, SCRAM,
Schema Registry, or MDS reads at all. A least-privilege credential therefore
needs describe rights only for the kinds its pipeline manages — this matters on
Confluent Cloud in particular, where API keys cannot describe client quotas at
all. See
[Least-privilege connecting principal](connecting.md#least-privilege-connecting-principal).

### verify

Same comparison as `diff`, but exits 1 when drift is found (suitable for CI).

```sh
monedula-gitops verify -f ./testdata/valid --cluster-config-file ./testdata/clusters/dev.yaml
```

### apply

Apply the manifests to the cluster. Add `--dry-run` to preview the planned
operations without mutating anything; `-o yaml` / `-o json` produce a
deterministic document.

```sh
# preview
monedula-gitops apply -f ./topics --cluster-config-file ./clusters/prod.yaml --dry-run

# execute
monedula-gitops apply -f ./topics --cluster-config-file ./clusters/prod.yaml
```

Add `--rotate-passwords` to also re-upsert the SCRAM password of every
declared, in-sync `KafkaUser` from its configured source. `KafkaUser` passwords
are never part of drift (Kafka's SCRAM-describe API cannot expose them), so
without this flag an in-sync user's password is left untouched even if the
underlying secret value changed; users with identity drift (username,
mechanism, or iterations out of sync) are updated regardless of the flag. It
composes with `--dry-run` like any other planned operation — the rotations are
shown, not applied.

```sh
monedula-gitops apply -f ./users --cluster-config-file ./clusters/prod.yaml --rotate-passwords
```

See [Applying changes](#applying-changes) for approval gates and exit codes.

### doctor

Check connectivity and operational readiness for one cluster without touching
any manifests: config parsing, broker connection (TLS + auth), Admin API
access, ACL read permissions, and Schema Registry reachability (skipped when
not configured). Each check reports `pass`/`fail`/`skip`; the command exits 0
when everything passes and 2 when any check fails. It never mutates Kafka.

```sh
monedula-gitops doctor --cluster-config-file ./clusters/prod.yaml
monedula-gitops doctor --cluster-config-file ./clusters --cluster prod-eu -o json
```

### import cluster

Reverse-engineer manifests from a live cluster. `import` reads live state only;
it **never mutates** Kafka. See [Importing](#importing) for namespace strategies,
overwrite modes, and the round-trip guarantee.

```sh
# render to stdout (manifests on stdout, summary on stderr — pipeable)
monedula-gitops import cluster --cluster-config-file ./clusters/prod.yaml > topics.yaml

# write a directory tree
monedula-gitops import cluster --cluster-config-file ./clusters/prod.yaml --output-dir ./imported
```

## Importing

`import cluster` reads the live topics and ACLs of a cluster and generates
`KafkaTopic` and `KafkaAccessPolicy` manifests. By default it renders to stdout
(manifests on stdout, the summary on stderr) so the manifest stream is
pipeable; pass `--output-dir <dir>` to write files instead. Imported files are
laid out under `<namespace>/{topics,access}/`, e.g.
`<dir>/default/topics/payments.orders.yaml`. Stdout mode is refused when the
cluster has schema subjects to import — schema bodies must be written as files,
so use `--output-dir` in that case.

Kafka/Confluent housekeeping topics (`__*` such as `__consumer_offsets`, plus
Confluent's `_schemas` and `_confluent*` topics) are skipped by default; pass
`--include-internal` to import them too.

Client quotas are reconstructed into `KafkaQuota` manifests. Pass
`--skip-quotas` to skip quota reconstruction entirely (no
`DescribeClientQuotas` call) — use it when quota describes are unsupported
(Confluent Cloud rejects them outright) or when quotas are externally managed.
The summary then reports `Quotas: skipped (--skip-quotas)` instead of a zero
count, mirroring `--skip-users`/`--skip-schemas`.

### Overwrite modes

When writing to `--output-dir`, `--overwrite` controls how existing files are
treated:

| Mode      | Behavior                                              |
|-----------|------------------------------------------------------|
| `never`   | (default) skip files that already exist.             |
| `changed` | overwrite only when the generated content differs.   |
| `always`  | overwrite unconditionally.                            |

### Namespace strategies

`--namespace-strategy` chooses how each topic is assigned a namespace:

| Strategy       | Flag(s)                                                       |
|----------------|--------------------------------------------------------------|
| `single`       | `--namespace <ns>` (default `default`).                       |
| `prefix`       | `--prefix-separator <sep>` (default `.`); the first segment.  |
| `regex`        | `--namespace-regex <re>` with one capture group.             |
| `mapping-file` | `--namespace-mapping-file <path>` (YAML/JSON topic→namespace).|

### SCRAM user reconstruction

`import cluster` also lists live SCRAM credentials and generates one
`KafkaUser` manifest per username (preferring `SCRAM-SHA-512` when a user has
credentials under both mechanisms simultaneously, with a warning naming the
uncaptured one). Since Kafka never exposes SCRAM passwords back out, the
generated `spec.password` is always a placeholder environment-variable
reference (`KAFKA_USER_<SANITIZED_USERNAME>_PASSWORD`) — the import summary
carries an explicit warning that these are **not recoverable** from the
cluster and the referenced env vars must be set (or the manifest switched to
`secretKeyRef`/`generate`) before the manifest can be applied. `spec.iterations`
is emitted only when the live value differs from the default (`4096`).

Two flags tune this behavior:

| Flag | Default | Effect |
|------|---------|--------|
| `--skip-users` | `false` | Skip SCRAM credential reconstruction entirely (no `ListScramCredentials` call) — use when users are managed by another application. |
| `--include-connecting-user` | `false` | Include the connecting principal's own SCRAM credential. Skipped by default to avoid a self-lockout risk (managing your own credential's `KafkaUser` manifest is an easy way to accidentally rotate yourself out). |

```sh
monedula-gitops import cluster --cluster-config-file ./clusters/prod.yaml --skip-users
```

### Round-trip guarantee

Importing a cluster and then running `verify` against the **same** cluster
reports **no drift** — the generated manifests are a faithful description of
live state. Topic partitions and configs mirror the live topic; topic-local
producer/consumer access is reconstructed by exact-match folding of the
corresponding ACLs onto the `KafkaTopic`, and advanced or ambiguous ACLs (for
example prefixed patterns) become standalone `KafkaAccessPolicy` rules, emitted
with warnings.

Note that the import output nests files under `<namespace>/topics/...`, so
`verify` must be run recursively against an imported directory:

```sh
monedula-gitops import cluster --cluster-config-file ./clusters/prod.yaml --output-dir ./imported
monedula-gitops verify -f ./imported -R --cluster-config-file ./clusters/prod.yaml   # exits 0: no drift
```

Schemas are imported too: every value/key subject that matches an imported
topic is folded into that topic's `spec.schema`, and the schema body is written
**verbatim** under `<namespace>/schemas/<topic>-value.<ext>` (referenced by a
relative `valueFrom.file`). Running `verify` against the same cluster then
reports no schema drift — see [Schema management](schemas.md).

## Applying changes

`apply` executes the diff against the cluster. By default risky operations are
**blocked** and must be explicitly approved:

| Flag                 | Permits                                                          |
|----------------------|------------------------------------------------------------------|
| `--allow-destructive`| Partition increase and other destructive changes.               |
| `--allow-delete`     | Topic deletion.                                                  |
| `--prune`            | Deletion of in-scope ACLs that are no longer in the manifests.   |

An operation whose gate is not satisfied is reported as `Blocked` and is not
executed. When blocked operations are the **only** problem, `apply` exits 3
(plan is sound, approval pending); any failed/skipped/rejected operation or
configuration error exits 2. Equivalent annotations on a resource
(`gitops.monedula.dev/allow-destructive`, `.../allow-delete`) are honored as
well.

ACL **pruning is opt-in** (Flux-style): without `--prune`, prune candidates are
reported as `PruneDisabled` — visible in diff/verify/apply output and counted
as drift by `verify` — but never deleted, and they do not fail the apply. This
protects against an accidentally truncated manifest cutting production access.

Execution is **best-effort** and never rolls back: independent operations
continue past failures, and ACLs belonging to a topic that failed to create are
reported as `Skipped`. Re-running `apply` converges the remaining changes. Each
operation's outcome (`Succeeded`, `Failed`, `Skipped`, `Blocked`, `Rejected`,
`PruneDisabled`) is listed in the result.

## Exit codes

| Code | Meaning                                                                 |
|------|------------------------------------------------------------------------|
| 0    | Success (or no drift for `verify`; all checks pass for `doctor`).       |
| 1    | Drift detected (`verify` only; drift on `ObserveOnly` resources is ignored). |
| 2    | Validation/configuration/connectivity error, an `apply` with any failed/skipped/rejected operation, or a failed `doctor` check. |
| 3    | `apply` only: the only non-OK operations are `Blocked` — the plan is sound but gated operations await approval (`--allow-delete` / `--allow-destructive`). |

Unconsented prune candidates (`PruneDisabled`) and report-only operations on
`DetectOnly`/`ObserveOnly` resources count as OK for `apply`.

## Shared model

The CLI and the operator are two front-ends over **one type set and one
diff/executor engine**. The API types (`KafkaCluster`, `KafkaTopic`,
`KafkaAccessPolicy`, `KafkaQuota`, `KafkaRoleBinding`, `KafkaUser`) are
Kubernetes-native (`metav1` metadata, deepcopy, a registered scheme), so the
very same Go structs back both `monedula-gitops apply` and the in-cluster
reconcilers. Diff computation, the risk gates, and execution against Kafka /
the Schema Registry are shared code — only the front matter differs: the CLI
resolves credentials from env/file and gates with flags, while the operator
resolves from Kubernetes Secrets and gates with annotations. `KafkaUser`
follows the same split for its password source (`env`/`file` for the CLI,
`secretKeyRef` for the operator), plus an operator-only `generate` mode with
no CLI equivalent — see [Manifest reference](manifest-reference.md#kafkauser-password).

## Multi-environment promotion

There is no built-in "environment" concept — `-f`/`--cluster-config-file` are
just paths, so dev→staging→prod promotion is a layout and rendering choice you
make in Git, not a tool feature. Two layouts work well:

**Per-env cluster config, shared manifests.** Keep one `cluster.yaml` per
environment and one manifest tree that both environments load as-is:

```text
env/
  dev/cluster.yaml
  staging/cluster.yaml
  prod/cluster.yaml
manifests/
  topics/orders.yaml
  access/checkout.yaml
```

```sh
monedula-gitops apply -f ./manifests --cluster-config-file env/staging/cluster.yaml
monedula-gitops apply -f ./manifests --cluster-config-file env/prod/cluster.yaml
```

This works as long as topic-level settings (partitions, retention) are meant
to be identical across environments. The moment prod needs more partitions or
longer retention than dev, you need per-env manifest rendering too.

**Kustomize overlay, rendered and piped into `apply`.** Keep a base manifest
plus per-env patches, and render with `kubectl kustomize` (or `kustomize
build`) before handing the result to the CLI over stdin:

```text
base/
  kustomization.yaml
  orders.yaml               # partitions: 3, retention.ms: "86400000"
overlays/
  prod/
    kustomization.yaml
    orders-patch.yaml        # partitions: 12, retention.ms: "604800000"
env/
  prod/cluster.yaml
```

```yaml
# overlays/prod/kustomization.yaml
resources:
  - ../../base
patches:
  - path: orders-patch.yaml
    target:
      kind: KafkaTopic
      name: orders
```

```yaml
# overlays/prod/orders-patch.yaml — strategic-merge patch overriding env-specific fields
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaTopic
metadata:
  name: orders
spec:
  partitions: 12
  config:
    retention.ms: "604800000"
```

```sh
kubectl kustomize overlays/prod | monedula-gitops apply -f - --cluster-config-file env/prod/cluster.yaml
```

`apply -f -` reads manifests from stdin, so any renderer (Kustomize, Helm
`template`, a script) that emits `KafkaTopic`/`KafkaAccessPolicy`/`KafkaQuota`/
`KafkaRoleBinding`/`KafkaUser` YAML on stdout composes with the CLI without a
temp file. The same substitution works for `diff`/`verify`.

**Verify per environment in CI.** Because promotion is just "render + point at
a different cluster config," gate each environment independently:

```sh
kubectl kustomize overlays/staging | monedula-gitops verify -f - --cluster-config-file env/staging/cluster.yaml
kubectl kustomize overlays/prod    | monedula-gitops verify -f - --cluster-config-file env/prod/cluster.yaml
```

A staging pipeline that passes `verify` does not guarantee prod will — the
overlay diverges deliberately — so run `verify` per environment rather than
inferring prod's state from staging's result.
