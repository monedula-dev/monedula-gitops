# Scenario 13 — Topic deletion (Orphan policy)

**Modes:** k8s  
**Cluster profile:** shared-sasl

## What this teaches

`deletionPolicy: Orphan` (the default) means deleting the `KafkaTopic` CR
removes the finalizer immediately and the operator leaves the Kafka topic intact
on the broker — "stop managing, don't destroy". The CR disappears from
Kubernetes, but the broker topic `orphan.demo` survives. This is useful when you
want to stop GitOps ownership of a topic without disrupting consumers that
depend on it.

Contrast: scenario 14 shows `deletionPolicy: Delete`, which instructs the
operator to remove the broker topic before releasing the finalizer — "stop
managing AND destroy".

## How to run

### k8s mode

Apply the manifest, confirm it reaches `Ready=True`, then delete the CR:

```bash
kubectl apply -f manifests/
kubectl wait kafkatopic/orphan-demo --for=condition=Ready --timeout=120s
kubectl delete kafkatopic orphan-demo
```

After deletion completes, confirm the broker topic still exists:

```bash
# Using the monedula-gitops e2e check command against the shared-sasl cluster:
monedula-gitops e2e check \
  --scenario . \
  --mode k8s \
  --cluster-config ../clusters/shared-sasl/cluster.yaml
```

## Expected outcome

After the `KafkaTopic` CR `orphan-demo` is deleted, the broker topic
`orphan.demo` survives on the Kafka broker. The finalizer is removed
immediately (no broker interaction), so `kubectl delete` completes quickly.
The `expect.yaml` asserts `liveState.topics` includes `orphan.demo`, confirming
Orphan semantics: the operator stops managing the topic without destroying it.

## Cleanup

```bash
./cleanup.sh k8s   # for k8s mode
```
