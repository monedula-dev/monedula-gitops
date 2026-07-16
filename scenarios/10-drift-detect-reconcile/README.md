# Scenario 10 — Detect and reconcile config drift

**Cluster:** `shared-sasl` | **Modes:** `cli`

## What this demonstrates

This scenario exercises the core GitOps detect-then-converge loop for topic config
drift. It shows that:

1. An initial `apply` brings the broker to the desired state (`cleanup.policy: compact`).
2. An out-of-band mutation (`e2e mutate`) simulates a human or automation changing
   the broker config directly — the classic GitOps drift event.
3. `verify` detects the drift and exits 1, naming the drifted field in its output,
   so CI pipelines can catch and surface divergence before it causes incidents.
4. A second `apply` reconciles the drift, restoring the declared state.
5. A final `verify` exits 0, confirming convergence.

## Expected outcome

| Step     | Command  | Exit | Meaning                                       |
|----------|----------|------|-----------------------------------------------|
| 1        | apply    | 0    | Topic created with `cleanup.policy: compact`  |
| 2        | mutate   | 0    | Broker drifted to `cleanup.policy: delete`    |
| 3        | verify   | 1    | Drift detected; `cleanup.policy` named        |
| 4        | apply    | 0    | Drift reconciled; topic restored to desired   |
| 5        | verify   | 0    | Clean — no drift remaining                    |

The `liveState` block asserts `cleanup.policy: compact` after all steps complete,
confirming the broker reflects the declared state.

## Contrast with scenario 11

Scenario 11 (`11-reconciliation-modes`) uses `reconciliation.mode: ObserveOnly`,
which means drift is always reported but `verify` never exits 1 and `apply` never
reconciles it. Here (scenario 10), the default `Enforce` mode means `verify` exits 1
on drift and `apply` corrects it.
