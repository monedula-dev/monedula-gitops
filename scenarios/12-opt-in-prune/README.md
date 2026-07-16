# Scenario 12 — Opt-in ACL pruning with --prune

**Cluster:** `shared-sasl` | **Modes:** `cli`

## What this demonstrates

This scenario teaches the opt-in ACL pruning safety model (spec §10.3):

- `apply` **never** deletes live ACLs silently when a manifest removes an access
  block. Instead it records them as `PruneDisabled` — visible in the output, but
  no deletion occurs. This is the safe default: an accidentally truncated manifest
  must not revoke access.
- `apply --prune` explicitly opts in to deleting in-scope ACLs that are no longer
  desired. This requires a deliberate, operator-supplied flag — it cannot happen
  automatically.

## Steps

| Step | Command                          | Exit | Meaning                                               |
|------|----------------------------------|------|-------------------------------------------------------|
| 1    | apply (manifests/)               | 0    | Topic + ACL for `User:svc-prune` created              |
| 2    | apply (manifests-pruned/)        | 0    | No access block; stale ACL reported as PruneDisabled  |
| 3    | apply (manifests-pruned/) --prune| 0    | `--prune` supplied; stale ACL deleted (Succeeded)     |
| 4    | verify (manifests-pruned/)       | 0    | Clean — no drift remaining                            |

## Expected outcome

- Step 2 output contains `PruneDisabled DeleteAcl` — the stale ACL is visible but
  not removed. The run still exits 0 because `PruneDisabled` is not a failure.
- Step 3 output contains `Succeeded DeleteAcl` — the ACL is actually deleted.
- Step 4 `verify` exits 0 confirming the broker and manifest are now in sync.

## Safety contract

Only `--prune` enables ACL deletion in CLI mode (spec §10.3). `--allow-destructive`
does NOT enable pruning — it gates partition increases and schema compatibility
lowering, not ACL deletion. This separation prevents an operator from accidentally
pruning ACLs when they intended to allow a topic-config mutation.
