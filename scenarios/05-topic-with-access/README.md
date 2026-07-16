# Scenario 05 — Topic with producer/consumer access

**Modes:** cli, k8s  
**Cluster profile:** shared-sasl

## What this teaches

A `KafkaTopic` can declare its own ACL rules inline via the `spec.access` block.
This scenario creates the topic `acl.orders` with a single producer
(`User:svc-producer`) granted `Write` and a single consumer
(`User:svc-consumer`) granted `Read` on both the topic and the consumer group
`acl-orders-cg`. The reconciler translates those access declarations into the
corresponding Kafka ACL entries, so application teams never have to author raw
ACL resources separately.

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
kubectl get kafkatopic acl-orders -o jsonpath='{.status.conditions}'
```

## Expected outcome

The topic `acl.orders` is created on the broker. Three ACL entries are
created: `Write` on topic `acl.orders` for `User:svc-producer`; `Read` on
topic `acl.orders` for `User:svc-consumer`; and `Read` on group `acl-orders-cg`
for `User:svc-consumer`. In k8s mode the `KafkaTopic` resource `acl-orders`
reaches `Ready=True` and `TopicAccessSynced=True`. The CLI exits 0 and prints
`acl.orders` in its output summary.

## Cleanup

```bash
./cleanup.sh cli   # for CLI mode
./cleanup.sh k8s   # for k8s mode
```
