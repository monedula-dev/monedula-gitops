# Scenario 08 — IP connection-rate quota

**Modes:** cli, k8s  
**Cluster profile:** shared-sasl

## What this teaches

A `KafkaQuota` with an `ip` entity limits the rate at which new connections can
be opened from a specific IP address. This scenario caps `10.0.0.7` to 100 new
connections per second (`connectionCreationRate: 100`). IP-based quotas are
useful for protecting the broker from connection storms from misbehaving clients
before they exhaust broker resources.

This scenario also serves as the regression guard for the franz adapter
`ListQuotas` IP-filter fix — ensuring that ip-entity quotas are correctly
round-tripped through the broker API and not dropped by a clientId-only filter.

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
kubectl get kafkaquota ip-quota -o jsonpath='{.status.conditions}'
```

## Expected outcome

A quota entry for IP `10.0.0.7` is created on the broker with
`connection_creation_rate=100`. In k8s mode the `KafkaQuota` resource `ip-quota`
reaches `Ready=True` and `QuotaSynced=True`. The CLI exits 0.

## Cleanup

```bash
./cleanup.sh cli   # for CLI mode
./cleanup.sh k8s   # for k8s mode
```
