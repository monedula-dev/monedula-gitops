# Scenario 14 — Topic deletion (Delete policy with approval)

**Modes:** k8s  
**Cluster profile:** shared-sasl

## What this teaches

Deleting a `KafkaTopic` CR with `deletionPolicy: Delete` AND the approval
annotation `gitops.monedula.dev/allow-delete: "true"` causes the operator's
finalizer to **remove the Kafka topic and its managed ACLs from the broker**
before releasing the CR.

Both conditions are required:

- Without `deletionPolicy: Delete` the operator defaults to `Orphan` and never
  touches the broker on deletion.
- Without the `gitops.monedula.dev/allow-delete: "true"` annotation the
  operator retains the finalizer even when `deletionPolicy: Delete` is set —
  the CR hangs in `Terminating` indefinitely. This annotation is a deliberate
  safety gate to prevent accidental data loss.

With both fields present, `kubectl delete kafkatopic delete-demo` completes only
after the finalizer has removed the broker topic `delete.demo` and any managed
ACLs.

Contrast with scenario 13 (`deletionPolicy: Orphan`): Orphan drops the
finalizer immediately and leaves the broker topic intact.

## How to run

### k8s mode

Apply the manifest, confirm it reaches `Ready=True`, then delete the CR:

```bash
kubectl apply -f manifests/
kubectl wait kafkatopic/delete-demo --for=condition=Ready --timeout=120s
kubectl delete kafkatopic delete-demo
```

After deletion completes, confirm the broker topic is gone:

```bash
# Using the monedula-gitops e2e check command against the shared-sasl cluster:
monedula-gitops e2e check \
  --scenario . \
  --mode k8s \
  --cluster-config ../clusters/shared-sasl/cluster.yaml
```

## Expected outcome

After the `KafkaTopic` CR `delete-demo` is deleted, the broker topic
`delete.demo` is **removed from the Kafka broker**. The finalizer interacts
with the broker to delete the topic (and any managed ACLs) before the CR is
allowed to disappear. The `expect.yaml` asserts `liveState.absent.topics`
includes `delete.demo`, confirming Delete semantics: the operator stops
managing the topic and destroys it.

## Cleanup

```bash
./cleanup.sh k8s   # for k8s mode
```
