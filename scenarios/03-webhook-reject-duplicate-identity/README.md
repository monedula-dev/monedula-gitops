# Scenario 03 — Admission webhook rejects a duplicate topic identity

<!-- NOTE: The admission surface tested here is asserted by the bats harness
     (Task 9) by capturing the kubectl apply stderr. It is NOT checked by
     `e2e check`, because the rejected object (b.yaml) is never persisted and
     therefore does not appear in conditions or live state. -->

**Modes:** k8s  
**Cluster profile:** shared-sasl

## What this teaches

Kafka topic identity — the pair `(cluster, topicName)` — must be unique within
the operator's scope. The validating webhook enforces this at admission time,
before the object is persisted to etcd. When two `KafkaTopic` resources claim
the same `spec.topicName` on the same cluster reference, the second `kubectl
apply` is rejected immediately with a clear error message.

This is a "bad case" scenario. It requires the operator to be running with
`--enable-webhooks` (the k8s e2e harness arranges this).

## How to run

Apply the first topic (succeeds):

```bash
kubectl apply -f manifests/a.yaml
```

Apply the conflicting second topic (rejected):

```bash
kubectl apply -f manifests/b.yaml
```

The second command should fail with an admission error in stderr.

## Expected outcome

`kubectl apply -f manifests/b.yaml` is **rejected** by the admission webhook.
The error message from the apiserver contains:

```
topic identity (cluster, topicName) must be unique
```

(Full message: `KafkaTopic <ns>/dup-orders-b conflicts with <ns>/dup-orders-a:
both resolve to topicName "dup.orders" on cluster "shared" ...; topic identity
(cluster, topicName) must be unique (spec §5.3)`)

The source of the rejection message is
`internal/operator/webhook/kafkatopic_webhook.go` — `checkIdentityUnique`.

## Cleanup

```bash
./cleanup.sh k8s
```
