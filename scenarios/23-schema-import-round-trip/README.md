# Scenario 23 — Schema import round-trip

**Modes:** cli  
**Cluster profile:** shared-sasl

## What this teaches

`monedula-gitops import cluster` reconstructs **schema subjects** from a live
Schema Registry, not just topics, ACLs, and quotas. Scenario 16 imports with
`--skip-schemas`, leaving schema round-trip fidelity unproven; this scenario
closes that gap.

It applies a `KafkaTopic` carrying an inline AVRO value schema, imports the live
cluster **with** schemas (no `--skip-schemas`), and re-verifies the imported
manifests. A drift-free `verify` proves the subject (`schemaimport.demo-value`),
the schema body, and the compatibility level (`BACKWARD`) all round-trip faithfully.

## How to run

### CLI mode

```bash
# 1. Apply the topic with its inline AVRO schema (registers schemaimport.demo-value).
monedula-gitops apply -f manifests/ \
  --cluster-config-file ../clusters/shared-sasl/cluster.yaml

# 2. Import the live cluster — schemas included — into a temp directory.
monedula-gitops import cluster \
  --cluster-config-file ../clusters/shared-sasl/cluster.yaml \
  --output-dir /tmp/imported

# 3. Verify the imported manifests (topic + reconstructed schema) are drift-free.
monedula-gitops verify -f /tmp/imported \
  --cluster-config-file ../clusters/shared-sasl/cluster.yaml
```

## Expected outcome

The final `verify` exits 0 (drift-free round-trip). Import faithfully
reconstructed the registered schema subject — body and compatibility — so the
reconciler finds no difference between the imported manifests and the live
Schema Registry.

## Cleanup

```bash
./cleanup.sh cli   # for CLI mode (no-op — broker is torn down with compose)
./cleanup.sh k8s   # for k8s mode
```
