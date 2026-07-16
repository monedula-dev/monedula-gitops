# Scenario 19 — Drift ignoreFields

**Cluster:** `shared-sasl` | **Modes:** `cli`

## What this demonstrates

This scenario proves that `spec.drift.ignoreFields` excludes specific config keys from
drift detection in `verify`, `diff`, and `apply`. It shows that:

1. An initial `apply` creates the topic with `cleanup.policy: compact` and
   `retention.ms: 604800000`, and registers `config.retention.ms` as an ignored field.
2. An out-of-band mutation (`e2e mutate`) changes `retention.ms` directly on the broker —
   simulating a human or automation modifying broker config without going through GitOps.
3. `verify` exits 0 because `config.retention.ms` is listed in `drift.ignoreFields`,
   so the divergence is not counted as drift. No reconciliation is triggered.

## How to run

```bash
# Structural validation only (no infra required):
go test ./internal/e2e/ -run TestSeedCatalogParses

# Live run against a running shared-sasl compose stack:
go test -tags e2e ./test/e2e/cli/ -run TestSharedSASLScenarios/19-drift-ignore-fields -v
```

## Expected outcome

| Step | Command | Exit | Meaning                                                     |
|------|---------|------|-------------------------------------------------------------|
| 1    | apply   | 0    | Topic created; `retention.ms` declared and registered       |
| 2    | mutate  | —    | Broker `retention.ms` changed to `999999` out of band       |
| 3    | verify  | 0    | No drift — `config.retention.ms` is in `ignoreFields`       |

## Contrast with scenario 10

Scenario 10 (`10-drift-detect-reconcile`) mutates `cleanup.policy`, which is **not**
in `ignoreFields`. `verify` exits 1 and names the drifted field, requiring a second
`apply` to reconcile. Here (scenario 19), `retention.ms` **is** ignored, so `verify`
exits 0 even though the broker value no longer matches the manifest — the field is
intentionally excluded from drift enforcement.
