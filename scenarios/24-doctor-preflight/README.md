# Scenario 24 — Doctor preflight checks

**Modes:** cli  
**Cluster profile:** shared-sasl

## What this teaches

`monedula-gitops doctor` runs read-only connectivity and operational-readiness probes against a
cluster — run it before `apply` to catch broker/Schema-Registry/credential problems early. It
reports each check (`config`, `kafka-connect`, `kafka-admin`, `acl-read`, `schema-registry`),
exits 0 when every check passes (skips are fine), and exits 2 if any check fails.

This scenario runs `doctor` against the healthy `shared-sasl` cluster and asserts the preflight is
clean: the Kafka admin API and Schema Registry are reachable and `Doctor: healthy` is reported.

## How to run

### CLI mode

```bash
monedula-gitops doctor \
  --cluster-config-file ../clusters/shared-sasl/cluster.yaml
```

## Expected outcome

Exit 0. Output lists each check as `PASS`, including `kafka-admin` and `schema-registry`, and ends
with `Doctor: healthy`. (Against an unreachable broker or bad credentials, `doctor` instead reports
the failing checks and exits 2 — covered by the command's unit tests.)

## Cleanup

```bash
./cleanup.sh cli   # no-op — doctor is read-only
./cleanup.sh k8s   # no-op
```
