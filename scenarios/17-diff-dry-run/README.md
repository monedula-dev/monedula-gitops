# Scenario 17 — Diff and dry-run preview

**Cluster:** `shared-sasl` | **Modes:** `cli`

## What this demonstrates

This scenario exercises two read-only preview commands: `diff` and `apply --dry-run`.
Neither command mutates the broker; they only render what an `apply` would do (or has
already done). It shows that:

1. `diff` before `apply` reports the planned operation — creating `diff.demo` — so
   operators can inspect the change set before committing it.
2. `apply` creates the topic and brings the broker to the desired state.
3. A second `diff` reports no drift (output contains "No changes"), confirming the
   broker already reflects the declared manifests.
4. `apply --dry-run` re-renders the same empty plan without touching the broker,
   giving a final machine-checkable confirmation that nothing would change.

The sequence is entirely safe to run in production clusters: `diff` and
`apply --dry-run` never write to Kafka.

## How to run

All four commands are issued from the `scenarios/17-diff-dry-run/` directory.
Replace `<repo>` with the path to your local repository root.

```bash
# Step 1 — preview the planned create (diff.demo will appear in output)
monedula-gitops diff \
  -f manifests/ \
  --cluster-config-file ../clusters/shared-sasl/cluster.yaml

# Step 2 — apply the desired state (creates diff.demo on the broker)
monedula-gitops apply \
  -f manifests/ \
  --cluster-config-file ../clusters/shared-sasl/cluster.yaml

# Step 3 — diff again (should report No changes)
monedula-gitops diff \
  -f manifests/ \
  --cluster-config-file ../clusters/shared-sasl/cluster.yaml

# Step 4 — dry-run apply (renders the empty plan, no broker writes)
monedula-gitops apply --dry-run \
  -f manifests/ \
  --cluster-config-file ../clusters/shared-sasl/cluster.yaml
```

## Expected outcome

| Step | Command           | Exit | Meaning                                               |
|------|-------------------|------|-------------------------------------------------------|
| 1    | diff              | 0    | Planned create of `diff.demo` shown in output         |
| 2    | apply             | 0    | Topic created; broker now matches desired state       |
| 3    | diff              | 0    | No changes — broker already reflects the manifests   |
| 4    | apply --dry-run   | 0    | No changes — dry-run confirms nothing would be done  |

Steps 1, 3, and 4 are read-only: they probe the broker but never write to it.
Step 2 is the only mutating operation. Steps 3 and 4 are therefore idempotent
and safe to repeat at any time.
