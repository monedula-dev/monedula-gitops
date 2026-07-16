# Scenario 11 — ObserveOnly reconciliation mode

**Cluster:** `shared-sasl` | **Modes:** `cli`

## What this demonstrates

This scenario contrasts with scenario 10 (`10-drift-detect-reconcile`) to teach the
difference between `Enforce` (the default) and `ObserveOnly` reconciliation modes.

With `reconciliation.mode: ObserveOnly`:

- `apply` **never creates and never enforces** — every operation (topic create,
  config update) is rendered as `ReportOnly` and skipped. An ObserveOnly manifest
  is a passive observer; it cannot bring a topic into existence.
- Because of that, this scenario first **seeds** the topic with a one-off `Enforce`
  apply (`manifests-enforce/`, the same topic with no `reconciliation.mode`), and
  only then switches to the ObserveOnly manifest (`manifests/`) for the
  observe/drift steps.
- Once observing, `apply` **never overwrites** the topic's config, even when it
  drifts — it renders the config op as `ReportOnly`, which is informational only.
- `verify` **always exits 0** for this topic, even when drift exists — drift on an
  ObserveOnly resource is informational and never a CI failure.
- The drift IS rendered in the verify output (you can see what changed), but the
  exit code stays 0, so the pipeline is not blocked.

## Expected outcome

| Step | Command                    | Exit | Meaning                                                       |
|------|----------------------------|------|--------------------------------------------------------------|
| 1    | apply (manifests-enforce/) | 0    | One-off Enforce apply seeds the topic (ObserveOnly can't)    |
| 2    | mutate                     | 0    | Broker drifted to `cleanup.policy: delete`                    |
| 3    | verify (manifests/)        | 0    | ObserveOnly: drift rendered in output; exit 0 (report-only)  |

## Contrast with scenario 10

Scenario 10 uses the default `Enforce` mode. Its `verify` step 3 exits 1 because
the drift on an Enforce resource is a real contract violation. Here, ObserveOnly
means the manifest is a passive observer — it records what the desired state should
be and reports divergence, but never enforces it and never blocks CI.

Use ObserveOnly for:
- Topics owned by another team that you want to audit without taking over.
- Read-only config documentation (the manifest is the source of truth for desired
  state, but enforcement is delegated elsewhere).
- Gradual migration: bring a topic under GitOps management in observe-then-enforce
  phases.
