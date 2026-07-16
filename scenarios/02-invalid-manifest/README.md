# Scenario 02 — Validation rejects a malformed manifest

**Modes:** cli  
**Cluster profile:** shared-sasl

## What this teaches

The `validate` command catches schema violations in manifests before any
cluster contact is attempted. This scenario exercises the lower bound on
`spec.partitions`: setting `partitions: 0` is caught locally, the command exits
with code 2, and the error is printed to stdout. No Kafka broker or Kubernetes
apiserver is contacted.

This is a "bad case" scenario — the goal is to confirm that the tooling
rejects invalid input cleanly and early.

## How to run

```bash
monedula-gitops validate -f manifests/
```

## Expected outcome

The command exits with code **2** and the output contains:

```
KafkaTopic bad-topic: spec.partitions must be >= 1
```

The exact message fragment `partitions must be >= 1` identifies the violated
constraint. No resources are created or modified.

## Cleanup

No cleanup required — validation does not write to the cluster.

```bash
./cleanup.sh cli
```
