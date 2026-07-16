# Scenario 09 — Register an AVRO schema

**Modes:** cli, k8s  
**Cluster profile:** shared-sasl

## What this teaches

A `KafkaTopic` can carry a `spec.schema` block that registers an AVRO (or other)
schema in Schema Registry as part of the same declarative apply. This scenario
creates the topic `schema.demo` and simultaneously registers a minimal AVRO
record schema under the subject `schema.demo-value` (the default TopicName
strategy subject) with `BACKWARD` compatibility. The schema is embedded inline
(`valueFrom.inline`) so it works in both CLI and operator modes without
requiring filesystem access.

## How to run

### CLI mode

```bash
monedula-gitops apply -f manifests/ \
  --cluster-config-file ../clusters/shared-sasl/cluster.yaml
```

### k8s mode

```bash
kubectl apply -f manifests/
```

Confirm reconciliation:

```bash
kubectl get kafkatopic schema-demo -o jsonpath='{.status.conditions}'
```

## Expected outcome

The topic `schema.demo` is created on the broker. The subject `schema.demo-value`
is registered in Schema Registry with `BACKWARD` compatibility. In k8s mode the
`KafkaTopic` resource `schema-demo` reaches `Ready=True` and `SchemaSynced=True`.
The CLI exits 0.

## Cleanup

```bash
./cleanup.sh cli   # for CLI mode
./cleanup.sh k8s   # for k8s mode
```
