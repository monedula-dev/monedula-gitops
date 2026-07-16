# Scenario 06 — Standalone access policy

**Modes:** cli, k8s  
**Cluster profile:** shared-sasl

## What this teaches

A `KafkaAccessPolicy` expresses ACLs that are decoupled from any particular
topic — useful for granting a service access to transactional IDs, consumer
groups, or cross-topic topic prefixes without embedding the rules in individual
`KafkaTopic` manifests. This scenario grants `User:svc-billing` the `Write` and
`Describe` operations on the transactional ID `billing-tx` and `Read` on the
consumer group `billing-cg`.

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
kubectl get kafkaaccesspolicy billing-access -o jsonpath='{.status.conditions}'
```

## Expected outcome

Three ACL entries are created on the broker: `Write` on transactionalId
`billing-tx` for `User:svc-billing`; `Describe` on transactionalId `billing-tx`
for `User:svc-billing`; and `Read` on group `billing-cg` for `User:svc-billing`.
In k8s mode the `KafkaAccessPolicy` resource `billing-access` reaches
`Ready=True` and `AccessPolicySynced=True`. The CLI exits 0.

## Cleanup

```bash
./cleanup.sh cli   # for CLI mode
./cleanup.sh k8s   # for k8s mode
```
