# Schema management

How a `KafkaTopic` declares and reconciles a schema against a Schema Registry,
using the same diff → verify → apply flow as topics and ACLs.

A `KafkaTopic` may declare a schema for its records via `spec.schema`. The
schema is reconciled against a Schema Registry using the same
diff → verify → apply flow as topics and ACLs.

```yaml
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaTopic
metadata:
  name: orders
spec:
  clusterRef:
    name: prod
  topicName: payments.orders
  partitions: 6
  schema:
    format: AVRO            # AVRO | JSON | PROTOBUF
    compatibility: BACKWARD # the subject-level compatibility level
    valueSchema:
      valueFrom:
        file: ./schemas/orders-value.avsc
    # keySchema is optional and works the same way:
    # keySchema:
    #   valueFrom:
    #     file: ./schemas/orders-key.avsc
```

- **Format** is one of `AVRO`, `JSON`, or `PROTOBUF`.
- **Subject strategy** (`spec.schema.subjectStrategy`): `TopicName` (default,
  subjects `<topicName>-value`/`-key`), `RecordName` (the schema's record full
  name), `TopicRecordName` (`<topicName>-<recordFullName>`), or `Custom` (named
  verbatim via `spec.schema.valueSubject` / `keySubject`).
- **Compatibility** sets the subject-level compatibility level (e.g. `BACKWARD`,
  `FORWARD`, `FULL`, `NONE`, and their `_TRANSITIVE` variants).
- **Governance mode (compatibility-only).** `valueSchema` is **optional**: a
  `spec.schema` with only `format` + `compatibility` (no `valueSchema`/`keySchema`)
  manages *only* the subject's compatibility level and never registers a schema
  version. Schema content is then owned by the producer's pipeline, which
  registers versions out of band — those versions are not treated as drift. This
  is the recommended default; supply a `valueSchema` only for contract-first
  (content-managed) topics.
- **Schema bodies** are supplied by file reference under `valueSchema` /
  `keySchema` (`valueFrom.file`), resolved **relative to the manifest** that
  declares them; in operator mode use `valueFrom.inline` or
  `valueFrom.configMapKeyRef`. The body is sent to the registry verbatim; the
  registry — not this tool — is authoritative for compatibility, so there is no
  local schema-syntax parsing.

## Schema lifecycle (additive + guarded)

Schema reconciliation follows the same gating philosophy as the rest of the
tool: additive and safety-increasing changes are ungated, while changes that
reduce safety are guarded behind `--allow-destructive`.

| Change                                         | Gated by             |
|------------------------------------------------|----------------------|
| Register a new schema version                  | (ungated)            |
| Raise compatibility (e.g. `BACKWARD` → `FULL`) | (ungated)            |
| Lower / move sideways (e.g. `FULL` → `BACKWARD`, `BACKWARD` → `FORWARD`) | `--allow-destructive` |
| First-time subject level **below the registry's global default** (e.g. `NONE` on a fresh subject under a global `BACKWARD`) | `--allow-destructive` |

A subject with no subject-level override *effectively* runs at the registry's
**global** default, so the first `compatibility:` you declare is classified
against that baseline (read once per run from `GET /config`); a first-time set
at or above the default is an ungated raise. If the global default cannot be
determined (very old registries), any first-time set is treated as a raise.
To proceed: pass `apply --allow-destructive` (CLI) or set the
`gitops.monedula.dev/allow-destructive: "true"` annotation (operator), or
choose a `compatibility` level at or above the registry's global default.

Removing `spec.schema` from a manifest does **not** delete the subject: existing
subjects are **orphaned**, never auto-deleted. Deleting the topic itself
(`deletionPolicy: Delete` + `--allow-delete`) deletes its **content-mode**
subjects; **governance-mode** subjects (compatibility-only) are never deleted,
since their content is producer-owned.

## Connecting to the Schema Registry

The registry endpoint and optional credentials live on the `KafkaCluster`:

```yaml
spec:
  bootstrapServers: broker1:9093,broker2:9093
  schemaRegistry:
    endpoint: https://schema-registry:8081
    auth:
      type: basic
      username:
        valueFrom:
          env: SR_USER
      password:
        valueFrom:
          file: /etc/secrets/sr-password
```

- `spec.schemaRegistry.endpoint` is required to enable schema reconciliation;
  a topic that declares `spec.schema` against a cluster with no registry
  endpoint is a configuration error.
- `spec.schemaRegistry.auth` is optional. Only `type: basic` is supported;
  `username`/`password` resolve as
  [secret references](connecting.md#connecting-to-kafka)
  (`valueFrom.env` or `valueFrom.file`). Resolved credentials are never logged.

## Schema import

`import cluster` reverse-engineers schemas alongside topics and ACLs. Subject
matching follows naming convention:

- **TopicName** subjects (`<topic>-value` / `<topic>-key`) are folded into
  `spec.schema` on the owning topic (as before).
- **TopicRecordName** subjects (`<topic>-<recordFullName>`) are reconstructed
  onto the owning topic with `subjectStrategy: TopicRecordName` (body-verified;
  value-by-default; ambiguity reported when two record-based subjects map to one topic).
- **RecordName** subjects (name equals record full name, no topic prefix) are
  **not** attributed to any topic — they surface in a "RecordName subjects
  needing manual attribution" report section (subject name, record name, schema
  type) in the import summary and are never written into a manifest.
- **Custom / hand-named subjects** remain in the generic unmatched-subjects warning.

Pass `--skip-schemas` to skip schema reconstruction entirely — no Schema Registry
connection is made and no schema data appears in the output or summary. Use this
when schemas are managed by producer applications or CI pipelines.

The schema body is written **verbatim** to
`<namespace>/schemas/<topic>-value.<ext>` (referenced from the topic via a
relative `valueFrom.file`). The round-trip holds end to end: importing a cluster
and then running `verify -R` against the **same** cluster reports no drift,
including the schema.

```sh
monedula-gitops import cluster --cluster-config-file ./clusters/prod.yaml --output-dir ./imported
monedula-gitops verify -f ./imported -R --cluster-config-file ./clusters/prod.yaml   # exits 0: no drift

# skip schema reconstruction (app-managed schemas)
monedula-gitops import cluster --cluster-config-file ./clusters/prod.yaml --output-dir ./imported --skip-schemas
```
