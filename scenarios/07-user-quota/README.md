# Scenario 07 — User quota

**Modes:** cli, k8s  
**Cluster profile:** shared-sasl

## What this teaches

A `KafkaQuota` with a `user` entity sets per-user throughput limits enforced by
the Kafka broker. This scenario caps `User:svc-quota-user` to 1 MiB/s for
produce traffic (`producerByteRate: 1048576`) and 2 MiB/s for consume traffic
(`consumerByteRate: 2097152`). Quotas are a critical knob for multi-tenant
clusters where runaway producers or consumers can saturate broker I/O.

Note: the manifest uses the Kafka principal form `User:svc-quota-user`; the live
broker quota API stores and returns the bare name `svc-quota-user` (the `User:`
prefix is stripped). The expect.yaml accordingly uses the bare name.

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
kubectl get kafkaquota svc-quota -o jsonpath='{.status.conditions}'
```

## Expected outcome

A quota entry for user `svc-quota-user` is created on the broker with
`producer_byte_rate=1048576` and `consumer_byte_rate=2097152`. In k8s mode the
`KafkaQuota` resource `svc-quota` reaches `Ready=True` and `QuotaSynced=True`.
The CLI exits 0.

## Cleanup

```bash
./cleanup.sh cli   # for CLI mode
./cleanup.sh k8s   # for k8s mode
```
