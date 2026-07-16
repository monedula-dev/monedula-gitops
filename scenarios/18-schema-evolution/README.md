# Scenario 18 — Schema evolution (compatibility governance)

**Cluster:** `shared-sasl` | **Modes:** `cli`

## What this demonstrates

This scenario proves Schema Registry compatibility governance end-to-end using
inline AVRO schemas:

1. **v1** registers the initial schema — a record `SchemaEvo` with a single
   `id` field of type `long`. `apply` succeeds and version 1 is stored in the
   registry.

2. **v2** adds an optional `label` field (type `string`, default `""`). Because
   a reader using the new schema can still decode records written by the old
   schema (the field has a default and is therefore optional for old writers),
   this change is **BACKWARD-compatible**. The registry accepts it as version 2;
   `apply` succeeds.

3. **bad** retypes `id` from `long` to `string`. Old readers expect a `long`
   wire encoding; the new type produces a `string` encoding — readers cannot
   cope. The registry rejects this change as **BACKWARD-incompatible** and
   returns an error containing `"compat"`. `apply` exits non-zero, proving the
   governance check fires.

All schemas are declared inline inside `spec.schema.valueSchema.valueFrom.inline`
(no external schema registry file required).

## How to run

Commands are issued from the repo root. Replace `<repo>` with your local path.

```bash
# Step 1 — register v1 (creates topic + registers initial schema)
monedula-gitops apply \
  -f scenarios/18-schema-evolution/manifests/ \
  --cluster-config-file scenarios/clusters/shared-sasl/cluster.yaml

# Step 2 — apply v2 (backward-compatible field addition — accepted)
monedula-gitops apply \
  -f scenarios/18-schema-evolution/manifests-v2/ \
  --cluster-config-file scenarios/clusters/shared-sasl/cluster.yaml

# Step 3 — apply bad (incompatible type change — rejected, exit 2)
monedula-gitops apply \
  -f scenarios/18-schema-evolution/manifests-bad/ \
  --cluster-config-file scenarios/clusters/shared-sasl/cluster.yaml
```

The last command is expected to fail.

## Expected outcome

| Step | Manifests dir   | Exit | Meaning                                                      |
|------|-----------------|------|--------------------------------------------------------------|
| 1    | `manifests/`    | 0    | v1 schema registered; topic `schema.evo` created            |
| 2    | `manifests-v2/` | 0    | v2 schema accepted (BACKWARD-compatible field with default)  |
| 3    | `manifests-bad/`| 2    | Incompatible change rejected; output contains `"compat"`     |

Step 3 failing with exit code 2 is the correct, desired behaviour. It confirms
that the CLI propagates Schema Registry compatibility errors to the caller.
