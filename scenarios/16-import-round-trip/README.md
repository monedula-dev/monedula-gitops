# Scenario 16 — Import round-trip

**Modes:** cli  
**Cluster profile:** shared-sasl

## What this teaches

`monedula-gitops import cluster` reads live broker state and reconstructs GitOps
manifests — topics, ACLs, quotas — into an output directory. Re-running `verify`
against those imported manifests proves the reconstruction is faithful: if import
captures state accurately, the reconciler sees no drift and exits 0.

This scenario applies a `KafkaTopic` with inline producer/consumer access plus a
`KafkaQuota`, imports with `--skip-schemas` (topics + ACLs + quotas; schema
round-trip is out of scope for this scenario), and asserts the imported set
re-verifies clean.

> **Note:** import reads the whole cluster, so the imported set may include other
> live topics present on the broker (from earlier scenarios that ran in the same
> compose session). `verify` only checks the resources in the imported set against
> the live broker, so those extra resources do not cause false failures — the
> round-trip stays faithful regardless of what else is on the cluster.

## How to run

### CLI mode

```bash
# 1. Apply the topic and quota.
monedula-gitops apply -f manifests/ \
  --cluster-config-file ../clusters/shared-sasl/cluster.yaml

# 2. Import the live cluster state into a temp directory.
monedula-gitops import cluster \
  --cluster-config-file ../clusters/shared-sasl/cluster.yaml \
  --output-dir /tmp/imported \
  --skip-schemas

# 3. Verify the imported manifests are drift-free.
monedula-gitops verify -f /tmp/imported \
  --cluster-config-file ../clusters/shared-sasl/cluster.yaml
```

## Expected outcome

The final `verify` exits 0 (drift-free round-trip). Import faithfully
reconstructed the live broker state, and the reconciler finds no difference
between the imported manifests and the broker.

## Cleanup

```bash
./cleanup.sh cli   # for CLI mode (no-op — broker is torn down with compose)
./cleanup.sh k8s   # for k8s mode
```
