# Scenario 01 — Create a topic with config

**Modes:** cli, k8s  
**Cluster profile:** shared-sasl

## What this teaches

This is the simplest possible declarative topic scenario. A single `KafkaTopic`
manifest is applied to the shared cluster and the reconciler (k8s mode) or CLI
(cli mode) ensures the physical Kafka topic `payments.orders` is created with
the requested configuration (`cleanup.policy=compact`). It establishes the
baseline: a well-formed manifest round-trips from YAML to a live Kafka topic
with no surprises.

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

Then confirm the operator reconciled the topic:

```bash
kubectl get kafkatopic payments-orders -o jsonpath='{.status.conditions}'
```

## Expected outcome

The topic `payments.orders` is created on the Kafka broker with
`cleanup.policy=compact` (partition count 3). In k8s mode the `KafkaTopic`
resource reaches `Ready=True`. The CLI exits 0 and prints `payments.orders` in
its output summary.

## Cleanup

```bash
./cleanup.sh cli   # for CLI mode
./cleanup.sh k8s   # for k8s mode
```
